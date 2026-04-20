package protocol

import (
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"shardingSphere-go/config"
	"shardingSphere-go/sqlrouter"
	"strconv"
	"strings"
	"time"
)

const (
	protocolVersion = 10
	serverVersion   = "8.0.28-shardingSphere-go"
	// Increase handshake timeout to accommodate client delays
	readTimeout = 60 * time.Second

	// capability flags
	clientLongPassword     = 0x00000001
	clientLongFlag         = 0x00000004
	clientProtocol41       = 0x00000200
	clientSecureConnection = 0x00008000
	clientMultiResults     = 0x00020000
	clientPsMultiResults   = 0x00040000
	clientPluginAuth       = 0x00080000
	clientDeprecateEOF     = 0x01000000
)

var (
	defaultCharset = byte(0x21) // utf8mb4_general_ci
	pluginName     = "mysql_native_password"
)

// MySQL command codes
const (
	comQuit                  = 0x01
	comInitDB                = 0x02
	comQuery                 = 0x03
	comFieldList             = 0x04
	comCreateDB              = 0x05
	comDropDB                = 0x06
	comReload                = 0x07
	comShutdown              = 0x08
	comStatistics            = 0x09
	comProcessInfo           = 0x0a
	comConnect               = 0x0b
	comProcessKill           = 0x0c
	comDebug                 = 0x0d
	comPing                  = 0x0e
	comTime                  = 0x0f
	comDelayedInsert         = 0x10
	comChangeUser            = 0x11
	comSetOption             = 0x1b
	comStmtPrepare           = 0x16
	comStmtExecute           = 0x17
	comStmtSendLongData      = 0x18
	comStmtClose             = 0x19
	comStmtReset             = 0x1a
	comSetVariable           = 0x22
	comSetVariableDeprecated = 0x27
)

// Prepared statement storage
var preparedStatements = make(map[uint32]*PreparedStatement)

// PreparedStatement represents a prepared statement
type PreparedStatement struct {
	ID         uint32
	SQL        string
	NumParams  int
	Params     []interface{}
	Columns    []string
	ParamTypes []byte
}

// mysql type constants (subset)
const (
	mysqlTypeVarString = 0xfd
)

func HandleHandshake(conn net.Conn) error {
	// 设置读取超时
	if err := conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
		log.Printf("Failed to set read deadline: %v", err)
		return err
	}

	// 发送握手包，返回 20 字节 scramble
	scramble, err := sendHandshakePacket(conn, 0)
	if err != nil {
		return err
	}

	// 接收认证包
	username, authResp, clientCaps, err := receiveAuthPacket(conn)
	if err != nil {
		log.Printf("Error during authentication packet read from %s: %v", conn.RemoteAddr(), err)
		return err
	}

	// 认证（校验 native token）
	if err := authenticateNative(username, authResp, scramble); err != nil {
		log.Printf("Authentication failed for user '%s' from %s: %v", username, conn.RemoteAddr(), err)
		return err
	}

	// 发送认证成功包（OK）
	if err := sendOKPacket(conn, 2, clientCaps); err != nil {
		return err
	}

	// Clear read deadline for session to avoid unintended timeouts
	_ = conn.SetReadDeadline(time.Time{})

	// 开始会话
	return RunSession(conn)
}

func sendHandshakePacket(conn net.Conn, seq byte) ([]byte, error) {
	var payload bytes.Buffer

	// 生成 20 字节的随机盐（8 + 12）
	salt1 := make([]byte, 8)
	salt2 := make([]byte, 12)
	if _, err := rand.Read(salt1); err != nil {
		return nil, err
	}
	if _, err := rand.Read(salt2); err != nil {
		return nil, err
	}
	scramble := append(append([]byte{}, salt1...), salt2...)

	// 服务器支持的能力位
	caps := uint32(0)
	caps |= clientLongPassword | clientLongFlag | clientProtocol41 | clientSecureConnection |
		clientMultiResults | clientPsMultiResults | clientPluginAuth | clientDeprecateEOF

	// HandshakeV10 payload
	payload.WriteByte(protocolVersion) // 1
	payload.WriteString(serverVersion) // NUL-terminated
	payload.WriteByte(0x00)
	payload.Write([]byte{0x01, 0x00, 0x00, 0x00}) // connection id
	payload.Write(salt1)                          // 8 bytes
	payload.WriteByte(0x00)                       // filler
	// lower 2 bytes capability flags
	payload.WriteByte(byte(caps))      // low
	payload.WriteByte(byte(caps >> 8)) // low-high
	payload.WriteByte(defaultCharset)  // character set
	payload.Write([]byte{0x02, 0x00})  // status flags (SERVER_STATUS_AUTOCOMMIT)
	// upper 2 bytes capability flags
	payload.WriteByte(byte(caps >> 16)) // high-low
	payload.WriteByte(byte(caps >> 24)) // high
	// auth plugin data length (总长度，通常 21)
	payload.WriteByte(21)
	// 10 字节保留
	payload.Write(make([]byte, 10))
	// auth-plugin-data-part-2: 至少 13 字节（salt2(12)+0x00）
	payload.Write(salt2)
	payload.WriteByte(0x00)
	// plugin name NUL-terminated
	payload.WriteString(pluginName)
	payload.WriteByte(0x00)

	if err := writePacket(conn, seq, payload.Bytes()); err != nil {
		return nil, err
	}
	return scramble, nil
}

func receiveAuthPacket(conn net.Conn) (string, []byte, uint32, error) {
	packet, seq, err := readPacket(conn)
	if err != nil {
		return "", nil, 0, err
	}
	_ = seq // 不强制使用

	pos := 0
	need := func(n int) error {
		if pos+n > len(packet) {
			return io.ErrUnexpectedEOF
		}
		return nil
	}

	// capability flags
	if err := need(4); err != nil {
		return "", nil, 0, err
	}
	caps := uint32(packet[pos]) | uint32(packet[pos+1])<<8 | uint32(packet[pos+2])<<16 | uint32(packet[pos+3])<<24
	pos += 4

	// max packet size
	if err := need(4); err != nil {
		return "", nil, 0, err
	}
	pos += 4

	// character set
	if err := need(1); err != nil {
		return "", nil, 0, err
	}
	pos += 1

	// 23 reserved
	if err := need(23); err != nil {
		return "", nil, 0, err
	}
	pos += 23

	// username (NUL-terminated)
	uEnd := bytes.IndexByte(packet[pos:], 0x00)
	if uEnd < 0 {
		return "", nil, 0, errors.New("invalid username")
	}
	username := string(packet[pos : pos+uEnd])
	pos += uEnd + 1

	// auth-response
	var authResp []byte
	switch {
	// CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
	case (caps & 0x00200000) != 0:
		l, n, ok := readLenEncInt(packet[pos:])
		if !ok {
			return "", nil, 0, errors.New("invalid lenenc auth-response")
		}
		pos += n
		if err := need(int(l)); err != nil {
			return "", nil, 0, err
		}
		authResp = append([]byte{}, packet[pos:pos+int(l)]...)
		pos += int(l)

	// CLIENT_SECURE_CONNECTION
	case (caps & clientSecureConnection) != 0:
		if err := need(1); err != nil {
			return "", nil, 0, err
		}
		l := int(packet[pos])
		pos++
		if err := need(l); err != nil {
			return "", nil, 0, err
		}
		authResp = append([]byte{}, packet[pos:pos+l]...)
		pos += l

	// 旧方式：NUL-terminated
	default:
		end := bytes.IndexByte(packet[pos:], 0x00)
		if end < 0 {
			return "", nil, 0, errors.New("invalid auth-response")
		}
		authResp = append([]byte{}, packet[pos:pos+end]...)
		pos += end + 1
	}

	// 可选：db, plugin name 等，这里略过解析

	return username, authResp, caps, nil
}

func authenticateNative(username string, authResp, scramble []byte) error {
	// Try authority config first (from global.yaml)
	if config.HasAuthorityConfig() {
		user, err := config.GetAuthorityUser(username)
		if err == nil {
			// Authenticate using authority user password
			pwd := []byte(user.Password)
			sha1pwd := sha1Sum(pwd)
			sha1sha1pwd := sha1Sum(sha1pwd)
			seed := make([]byte, 20)
			copy(seed, scramble[:20])
			check := sha1Sum(append(seed, sha1sha1pwd...))
			expected := xorBytes(sha1pwd, check)

			if len(authResp) != len(expected) {
				return errors.New("authentication failed: invalid token length")
			}
			for i := range expected {
				if authResp[i] != expected[i] {
					return errors.New("authentication failed: token mismatch")
				}
			}
			log.Printf("User '%s' authenticated successfully via authority config", username)
			return nil
		}
	}

	// Fallback to proxyUser config (from config.yaml)
	user, err := config.GetProxyUser(username)
	if err != nil {
		return errors.New("authentication failed: " + err.Error())
	}
	// mysql_native_password 验证:
	// token = sha1(password) XOR sha1(scramble + sha1(sha1(password)))
	pwd := []byte(user.Password)
	sha1pwd := sha1Sum(pwd)
	sha1sha1pwd := sha1Sum(sha1pwd)
	scr := append([]byte{}, scramble...)
	scr = append(scr, 0x00)
	seed := make([]byte, 20)
	copy(seed, scramble[:20])
	check := sha1Sum(append(seed, sha1sha1pwd...))
	expected := xorBytes(sha1pwd, check)

	if len(authResp) != len(expected) {
		return errors.New("authentication failed: invalid token length")
	}
	for i := range expected {
		if authResp[i] != expected[i] {
			return errors.New("authentication failed: token mismatch")
		}
	}
	return nil
}

func sendOKPacket(conn net.Conn, seq byte, clientCaps uint32) error {
	var payload bytes.Buffer
	payload.WriteByte(0x00) // OK header

	// affected_rows, last_insert_id (lenenc=0)
	payload.WriteByte(0x00)
	payload.WriteByte(0x00)

	// status_flags (SERVER_STATUS_AUTOCOMMIT)
	payload.Write([]byte{0x02, 0x00})

	// warnings
	payload.Write([]byte{0x00, 0x00})

	return writePacket(conn, seq, payload.Bytes())
}

func sendOKPacketWithStats(conn net.Conn, seq byte, affected, lastInsertID int64) error {
	var payload bytes.Buffer
	payload.WriteByte(0x00) // OK header

	// affected_rows (lenenc)
	writeLenEncInt(&payload, uint64(affected))
	// last_insert_id (lenenc)
	writeLenEncInt(&payload, uint64(lastInsertID))

	// status_flags (SERVER_STATUS_AUTOCOMMIT)
	payload.Write([]byte{0x02, 0x00})

	// warnings
	payload.Write([]byte{0x00, 0x00})

	return writePacket(conn, seq, payload.Bytes())
}

func writeErrPacket(conn net.Conn, seq byte, code int, sqlState, msg string) error {
	var payload bytes.Buffer
	payload.WriteByte(0xff)            // ERR header
	payload.WriteByte(byte(code))      // error code low
	payload.WriteByte(byte(code >> 8)) // error code high
	payload.WriteByte('#')             // SQL state marker
	payload.WriteString(sqlState)      // 5 bytes sql state
	payload.WriteString(msg)           // error message
	return writePacket(conn, seq, payload.Bytes())
}

func writePacket(conn net.Conn, seq byte, data []byte) error {
	length := len(data)
	header := []byte{byte(length), byte(length >> 8), byte(length >> 16), seq}
	packet := append(header, data...)
	_, err := conn.Write(packet)
	return err
}

func readPacket(conn net.Conn) ([]byte, byte, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		if err == io.EOF {
			log.Printf("Client %s disconnected: EOF", conn.RemoteAddr())
		} else {
			log.Printf("Error reading packet header from %s: %v", conn.RemoteAddr(), err)
		}
		return nil, 0, err
	}
	length := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
	seq := header[3]
	if length <= 0 {
		return nil, 0, errors.New("invalid packet length")
	}
	packet := make([]byte, length)
	if _, err := io.ReadFull(conn, packet); err != nil {
		if err == io.EOF {
			log.Printf("Client %s disconnected while reading packet body: EOF", conn.RemoteAddr())
		} else {
			log.Printf("Error reading packet body from %s: %v", conn.RemoteAddr(), err)
		}
		return nil, 0, err
	}
	return packet, seq, nil
}

// RunSession keeps reading command packets and responds accordingly.
func RunSession(conn net.Conn) error {
	for {
		// Do not set deadlines here; let the read block until client sends data
		packet, seq, err := readPacket(conn)
		if err != nil {
			return err
		}
		if len(packet) == 0 {
			continue
		}
		cmd := packet[0]
		switch cmd {
		case comQuit:
			// client requests close
			log.Printf("Client %s requested quit", conn.RemoteAddr())
			return nil
		case comPing:
			// respond OK
			if err := sendOKPacket(conn, seq+1, 0); err != nil {
				return err
			}
		case comInitDB:
			// Change default schema/database
			dbName := strings.TrimSpace(strings.Trim(string(packet[1:]), "\x00"))
			configuredDB := config.GetDatabaseName()
			log.Printf("Switching database to: '%s' (len=%d), configured: '%s' (len=%d)", dbName, len(dbName), configuredDB, len(configuredDB))
			// Validate database name
			if dbName != configuredDB && dbName != "information_schema" {
				log.Printf("Database mismatch: '%s' != '%s'", dbName, configuredDB)
				if err := writeErrPacket(conn, seq+1, 1049, "42000", fmt.Sprintf("Unknown database '%s'", dbName)); err != nil {
					return err
				}
				continue
			}
			// Acknowledge with OK
			if err := sendOKPacket(conn, seq+1, 0); err != nil {
				return err
			}
			continue
		case comSetOption:
			// Handle SET OPTION
			if len(packet) > 1 {
				option := uint16(packet[1]) | uint16(packet[2])<<8
				log.Printf("SET OPTION: %d", option)
			}
			if err := sendOKPacket(conn, seq+1, 0); err != nil {
				return err
			}
		case comSetVariable, comSetVariableDeprecated:
			// Handle SET variable = value
			sql := string(packet[1:])
			log.Printf("SET variable: %s", sql)
			if err := sendOKPacket(conn, seq+1, 0); err != nil {
				return err
			}
		case comStmtPrepare:
			// Prepared statement prepare
			sql := string(packet[1:])
			if err := handlePrepare(conn, seq+1, sql); err != nil {
				if werr := writeErrPacket(conn, seq+1, 1064, "HY000", "Error preparing statement: "+err.Error()); werr != nil {
					return werr
				}
			}
		case comStmtExecute:
			// Prepared statement execute
			if err := handleExecute(conn, seq+1, packet[1:]); err != nil {
				if werr := writeErrPacket(conn, seq+1, 1064, "HY000", "Error executing statement: "+err.Error()); werr != nil {
					return werr
				}
			}
		case comStmtClose:
			// Prepared statement close
			if len(packet) >= 5 {
				stmtID := uint32(packet[1]) | uint32(packet[2])<<8 | uint32(packet[3])<<16 | uint32(packet[4])<<24
				delete(preparedStatements, stmtID)
				log.Printf("Closed prepared statement %d", stmtID)
			}
		case comStmtReset:
			// Prepared statement reset
			if len(packet) >= 5 {
				stmtID := uint32(packet[1]) | uint32(packet[2])<<8 | uint32(packet[3])<<16 | uint32(packet[4])<<24
				if ps, exists := preparedStatements[stmtID]; exists {
					ps.Params = nil
				}
				if err := sendOKPacket(conn, seq+1, 0); err != nil {
					return err
				}
			}
		case comStatistics:
			// Show statistics
			stats := "Uptime: 3600  Threads: 1  Questions: 10  Slow queries: 0  Opens: 100  Flush tables: 1  Open tables: 50"
			if err := sendStringPacket(conn, seq+1, stats); err != nil {
				return err
			}
		case comProcessInfo:
			// Show process list
			processList := "Id\tUser\tHost\tdb\tCommand\tTime\tState\tInfo\n"
			if err := sendStringPacket(conn, seq+1, processList); err != nil {
				return err
			}
		case comFieldList:
			// Field list for a table
			if len(packet) > 1 {
				tableName := string(packet[1:])
				log.Printf("Field list request for: %s", tableName)
				// Return empty result set (table columns would be here)
				if err := writeLengthEncodedIntPacket(conn, seq+1, 0); err != nil {
					return err
				}
			}
		case comCreateDB, comDropDB:
			// Create/Drop database
			if err := sendOKPacket(conn, seq+1, 0); err != nil {
				return err
			}
		case comReload:
			// Reload privileges
			if err := sendOKPacket(conn, seq+1, 0); err != nil {
				return err
			}
		case comShutdown:
			// Shutdown
			log.Printf("Shutdown requested")
			return nil
		case comDebug:
			// Debug
			if err := sendOKPacket(conn, seq+1, 0); err != nil {
				return err
			}
		case comTime:
			// Send server time
			if err := sendOKPacket(conn, seq+1, 0); err != nil {
				return err
			}
		case comConnect:
			// Connection handshake
			if err := sendOKPacket(conn, seq+1, 0); err != nil {
				return err
			}
		case comProcessKill:
			// Kill a process
			if err := sendOKPacket(conn, seq+1, 0); err != nil {
				return err
			}
		case comQuery:
			// SQL string follows command byte
			sql := string(packet[1:])

			// Execute and get structured result
			res, err := sqlrouter.RouteAndExecuteSQLWithResult(sql)
			if err != nil {
				if werr := writeErrPacket(conn, seq+1, 1064, "HY000", "Error executing SQL: "+err.Error()); werr != nil {
					return werr
				}
				continue
			}

			// If this is not a query (no columns), send OK with affected rows/last insert id
			if len(res.Columns) == 0 {
				if err := sendOKPacketWithStats(conn, seq+1, res.Affected, res.LastInsertID); err != nil {
					return err
				}
				continue
			}

			// Send text result set (column count, column definitions, rows, EOF)
			// Column count
			if err := writeLengthEncodedIntPacket(conn, seq+1, uint64(len(res.Columns))); err != nil {
				return err
			}
			seq++

			// Column definition packets
			for _, col := range res.Columns {
				// Minimal column definition: catalog, schema, table, org_table, name, org_name, length, type, flags, decimals
				// Fill with empty strings where unknown; type as VAR_STRING
				if err := writeColumnDefinitionPacket(conn, seq+1,
					"def", "", "", "", col, col, 256, mysqlTypeVarString, 0, 0); err != nil {
					return err
				}
				seq++
			}

			// EOF after columns (for pre-5.7 behavior) or OK depending on server capability; use EOF for compatibility
			if err := writeEOFPacket(conn, seq+1); err != nil {
				return err
			}
			seq++

			// Row packets
			for _, row := range res.Rows {
				// Length-encoded strings for each column
				if err := writeTextRowPacket(conn, seq+1, res.Columns, row); err != nil {
					return err
				}
				seq++
			}

			// EOF after rows
			if err := writeEOFPacket(conn, seq+1); err != nil {
				return err
			}
		default:
			// unsupported command
			log.Printf("Unsupported command: 0x%02x", cmd)
			if err := writeErrPacket(conn, seq+1, 1235, "42000", "Unsupported command"); err != nil {
				return err
			}
		}
	}
}

// handlePrepare handles prepared statement preparation
func handlePrepare(conn net.Conn, seq byte, sql string) error {
	stmtID := uint32(len(preparedStatements) + 1)

	// Parse SQL to determine parameter count
	numParams := countPlaceholders(sql)

	ps := &PreparedStatement{
		ID:        stmtID,
		SQL:       sql,
		NumParams: numParams,
	}
	preparedStatements[stmtID] = ps

	// Send prepared OK packet
	var payload bytes.Buffer
	payload.WriteByte(0x00)                     // OK header
	writeLenEncInt(&payload, 0)                 // Statement ID
	writeLenEncInt(&payload, 0)                 // Column count
	writeLenEncInt(&payload, uint64(numParams)) // Parameter count
	writeLenEncInt(&payload, 0)                 // Reserved (always 0)

	if err := writePacket(conn, seq, payload.Bytes()); err != nil {
		return err
	}

	// Send parameter column definitions
	for i := 0; i < numParams; i++ {
		if err := writeColumnDefinitionPacket(conn, seq+1,
			"def", "", "", "", fmt.Sprintf("param_%d", i+1), "", 256, mysqlTypeVarString, 0, 0); err != nil {
			return err
		}
	}

	// Send EOF
	if numParams > 0 {
		if err := writeEOFPacket(conn, seq+1); err != nil {
			return err
		}
	}

	return nil
}

// handleExecute handles prepared statement execution
func handleExecute(conn net.Conn, seq byte, data []byte) error {
	if len(data) < 5 {
		return errors.New("invalid execute packet")
	}

	stmtID := uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16 | uint32(data[3])<<24
	ps, exists := preparedStatements[stmtID]
	if !exists {
		return errors.New("prepared statement not found")
	}

	// Execute the SQL
	res, err := sqlrouter.RouteAndExecuteSQLWithResult(ps.SQL)
	if err != nil {
		return err
	}

	// Send result set
	if len(res.Columns) == 0 {
		return sendOKPacketWithStats(conn, seq, res.Affected, res.LastInsertID)
	}

	// Column count
	if err := writeLengthEncodedIntPacket(conn, seq, uint64(len(res.Columns))); err != nil {
		return err
	}

	// Column definitions
	for _, col := range res.Columns {
		if err := writeColumnDefinitionPacket(conn, seq+1,
			"def", "", "", "", col, col, 256, mysqlTypeVarString, 0, 0); err != nil {
			return err
		}
		seq++
	}

	// EOF
	if err := writeEOFPacket(conn, seq+1); err != nil {
		return err
	}
	seq++

	// Rows
	for _, row := range res.Rows {
		if err := writeTextRowPacket(conn, seq+1, res.Columns, row); err != nil {
			return err
		}
		seq++
	}

	// EOF
	return writeEOFPacket(conn, seq+1)
}

// countPlaceholders counts the number of ? placeholders in SQL
func countPlaceholders(sql string) int {
	count := 0
	for _, c := range sql {
		if c == '?' {
			count++
		}
	}
	return count
}

// sendStringPacket sends a simple string packet
func sendStringPacket(conn net.Conn, seq byte, s string) error {
	return writePacket(conn, seq, []byte(s))
}

// countPlaceholders counts placeholders - duplicate needed for protocol package

// 工具函数
func sha1Sum(b []byte) []byte {
	h := sha1.New()
	h.Write(b)
	return h.Sum(nil)
}

func xorBytes(a, b []byte) []byte {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	out := make([]byte, n)
	for i := 0; i < n; i++ {
		out[i] = a[i] ^ b[i]
	}
	return out
}

func readLenEncInt(b []byte) (uint64, int, bool) {
	if len(b) == 0 {
		return 0, 0, false
	}
	switch b[0] {
	case 0xfc:
		if len(b) < 3 {
			return 0, 0, false
		}
		return uint64(b[1]) | uint64(b[2])<<8, 3, true
	case 0xfd:
		if len(b) < 4 {
			return 0, 0, false
		}
		return uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16, 4, true
	case 0xfe:
		if len(b) < 9 {
			return 0, 0, false
		}
		var v uint64
		for i := 0; i < 8; i++ {
			v |= uint64(b[1+i]) << (8 * uint(i))
		}
		return v, 9, true
	default:
		return uint64(b[0]), 1, true
	}
}

func writeLengthEncodedIntPacket(conn net.Conn, seq byte, v uint64) error {
	var payload bytes.Buffer
	writeLenEncInt(&payload, v)
	return writePacket(conn, seq, payload.Bytes())
}

func writeColumnDefinitionPacket(conn net.Conn, seq byte,
	catalog, schema, table, orgTable, name, orgName string,
	length uint32, colType byte, flags uint16, decimals byte,
) error {
	var payload bytes.Buffer

	// Protocol::ColumnDefinition41 fields (all length-encoded strings)
	writeLenEncString(&payload, catalog)  // catalog (usually "def")
	writeLenEncString(&payload, schema)   // schema
	writeLenEncString(&payload, table)    // table
	writeLenEncString(&payload, orgTable) // org_table
	writeLenEncString(&payload, name)     // name
	writeLenEncString(&payload, orgName)  // org_name

	// length of fixed-length fields (always 0x0c for MySQL 4.1+)
	payload.WriteByte(0x0c)

	// character set (2 bytes) - use utf8mb4_general_ci (0x21)
	payload.WriteByte(defaultCharset)
	payload.WriteByte(0x00)

	// column length (4 bytes little-endian)
	payload.WriteByte(byte(length))
	payload.WriteByte(byte(length >> 8))
	payload.WriteByte(byte(length >> 16))
	payload.WriteByte(byte(length >> 24))

	// type (1 byte)
	payload.WriteByte(colType)

	// flags (2 bytes little-endian)
	payload.WriteByte(byte(flags))
	payload.WriteByte(byte(flags >> 8))

	// decimals (1 byte)
	payload.WriteByte(decimals)

	// filler (2 bytes)
	payload.Write([]byte{0x00, 0x00})

	return writePacket(conn, seq, payload.Bytes())
}

func writeEOFPacket(conn net.Conn, seq byte) error {
	var payload bytes.Buffer
	payload.WriteByte(0xfe)           // EOF header
	payload.Write([]byte{0x00, 0x00}) // warnings
	payload.Write([]byte{0x02, 0x00}) // status_flags (SERVER_STATUS_AUTOCOMMIT)
	return writePacket(conn, seq, payload.Bytes())
}

func writeTextRowPacket(conn net.Conn, seq byte, columns []string, row map[string]interface{}) error {
	var payload bytes.Buffer
	for _, col := range columns {
		v := row[col]
		// Convert value to string for text protocol
		switch vv := v.(type) {
		case nil:
			// NULL is encoded as 0xfb (NULL in length-encoded format)
			payload.WriteByte(0xfb)
			continue
		case []byte:
			writeLenEncBytes(&payload, vv)
		case string:
			writeLenEncString(&payload, vv)
		default:
			// Fallback: format using %v
			writeLenEncString(&payload, toString(vv))
		}
	}
	return writePacket(conn, seq, payload.Bytes())
}

func writeLenEncInt(buf *bytes.Buffer, v uint64) {
	switch {
	case v < 0xfb:
		buf.WriteByte(byte(v))
	case v <= 0xffff:
		buf.WriteByte(0xfc)
		buf.WriteByte(byte(v))
		buf.WriteByte(byte(v >> 8))
	case v <= 0xffffff:
		buf.WriteByte(0xfd)
		buf.WriteByte(byte(v))
		buf.WriteByte(byte(v >> 8))
		buf.WriteByte(byte(v >> 16))
	default:
		buf.WriteByte(0xfe)
		for i := 0; i < 8; i++ {
			buf.WriteByte(byte(v >> (8 * uint(i))))
		}
	}
}

func writeLenEncString(buf *bytes.Buffer, s string) {
	writeLenEncInt(buf, uint64(len(s)))
	buf.WriteString(s)
}

func writeLenEncBytes(buf *bytes.Buffer, b []byte) {
	writeLenEncInt(buf, uint64(len(b)))
	buf.Write(b)
}

func toString(v interface{}) string {
	// Minimal conversion avoiding heavy dependencies
	switch x := v.(type) {
	case bool:
		if x {
			return "1"
		}
		return "0"
	case int:
		return strconv.Itoa(x)
	case int8:
		return strconv.FormatInt(int64(x), 10)
	case int16:
		return strconv.FormatInt(int64(x), 10)
	case int32:
		return strconv.FormatInt(int64(x), 10)
	case int64:
		return strconv.FormatInt(x, 10)
	case uint:
		return strconv.FormatUint(uint64(x), 10)
	case uint8:
		return strconv.FormatUint(uint64(x), 10)
	case uint16:
		return strconv.FormatUint(uint64(x), 10)
	case uint32:
		return strconv.FormatUint(uint64(x), 10)
	case uint64:
		return strconv.FormatUint(x, 10)
	case float32:
		return strconv.FormatFloat(float64(x), 'g', -1, 32)
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	case time.Time:
		// Use MySQL DATETIME string format
		return x.Format("2006-01-02 15:04:05")
	default:
		return fmt.Sprintf("%v", v)
	}
}
