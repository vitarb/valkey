# Test for handling large ping extensions (>64k)

start_cluster 1 0 {tags {external:skip cluster}} {

    proc hex16_be {n} { format %02x%02x [expr {($n>>8)&0xFF}] [expr {$n&0xFF}] }
    proc hex32_be {n} { format %02x%02x%02x%02x [expr {($n>>24)&0xFF}] [expr {($n>>16)&0xFF}] [expr {($n>>8)&0xFF}] [expr {$n&0xFF}] }
    proc hex64_be {n} { set hi [expr {($n>>32) & 0xFFFFFFFF}]; set lo [expr {$n & 0xFFFFFFFF}]; return [hex32_be $hi][hex32_be $lo] }

    proc send_large_ping {host port size} {
        set ext_padded [expr {(($size + 1 + 7)/8)*8}]
        set ext_total [expr {$ext_padded + 8}]
        set totlen [expr {2256 + $ext_total}]
        set hex ""
        append hex "52436d62" ;# 'RCmb'
        append hex [hex32_be $totlen]
        append hex [hex16_be 1]
        append hex [hex16_be 0]   ;# port
        append hex [hex16_be 0]   ;# type PING
        append hex [hex16_be 0]   ;# count
        append hex [hex64_be 0]
        append hex [hex64_be 0]
        append hex [hex64_be 0]
        append hex [string repeat 41 40] ;# sender id 'A'*40
        append hex [string repeat 00 2048] ;# slots
        append hex [string repeat 00 40]   ;# replicaof
        append hex [string repeat 00 46]   ;# myip
        append hex [hex16_be 1]            ;# extensions
        append hex [string repeat 00 30]
        append hex [hex16_be 0]            ;# pport
        append hex [hex16_be 0]            ;# cport
        append hex [hex16_be 0]            ;# flags
        append hex "00"                    ;# state
        append hex "04"                    ;# mflags[0] = EXT_DATA
        append hex "00"                    ;# mflags[1]
        append hex "00"                    ;# mflags[2]

        set ext_payload "[string repeat A $size]\x00"
        set pad_len [expr {$ext_padded - [string length $ext_payload]}]
        append ext_payload [string repeat "\x00" $pad_len]
        set ext_hex ""
        append ext_hex [hex32_be $ext_total]
        append ext_hex [hex16_be 0]
        append ext_hex [hex16_be 0]
        append ext_hex [binary encode hex $ext_payload]

        set packet [binary decode hex "$hex$ext_hex"]
        set sock [socket $host $port]
        fconfigure $sock -translation binary -blocking 1
        puts -nonewline $sock $packet
        flush $sock
        close $sock
    }

    test "Ping extension larger than 64k is accepted" {
        set host [srv 0 host]
        set cport [expr {[srv 0 port] + 10000}]
        set logline [count_log_lines 0]
        send_large_ping $host $cport 70000
        after 1000
        verify_no_log_message 0 "*Received invalid PING packet*" $logline
    }
}
