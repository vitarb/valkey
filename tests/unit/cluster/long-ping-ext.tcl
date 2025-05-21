# Test for handling large ping extensions (>64k)

start_cluster 1 0 {tags {external:skip cluster}} {
    proc build_large_ping {size} {
        set padded [expr {(($size + 1 + 7)/8)*8}]
        set ext_total [expr {$padded + 8}]
        set totlen [expr {2256 + $ext_total}]
        set hdr [binary format {A4 I S S S S W W W A40 A2048 A40 A46 S A30 S S S c c c c} \
            RCmb $totlen 1 0 0 0 0 0 0 \
            [string repeat A 40] \
            [string repeat \0 2048] \
            [string repeat \0 40] \
            [string repeat \0 46] \
            1 [string repeat \0 30] 0 0 0 0 4 0 0]
        set payload [string repeat A $size]
        append payload \x00
        append payload [string repeat \x00 [expr {$padded - [string length $payload]}]]
        set ext [binary format {I S S a*} $ext_total 0 0 $payload]
        return "$hdr$ext"
    }

    test "Ping extension larger than 64k is accepted" {
        set host [srv 0 host]
        set cport [expr {[srv 0 port] + 10000}]
        set sock [socket $host $cport]
        fconfigure $sock -translation binary -blocking 1
        set logline [count_log_lines 0]
        puts -nonewline $sock [build_large_ping 70000]
        flush $sock
        close $sock
        after 1000
        verify_no_log_message 0 "*Received invalid PING packet*" $logline
    }
}
