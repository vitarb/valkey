# Slab reads on query threads: a read takes effect at its command's position, whatever
# happens to the key while it waits, and its reply precedes later invalidations.

proc slab_fill {coll n tag {prefix k}} {
    for {set i 0} {$i < $n} {incr i 100} {
        set args {}
        for {set j $i} {$j < $i + 100 && $j < $n} {incr j} {
            lappend args [format "%s%06d" $prefix $j] "v$j:$tag"
        }
        r slmset $coll {*}$args
    }
}

proc slab_stat {field} {
    return [getInfoProperty [r info slab debug] $field]
}

proc slab_keys {n} {
    set keys {}
    for {set i 0} {$i < $n} {incr i} { lappend keys [format k%06d $i] }
    return $keys
}

# Sends a held 100-key read from a new deferring client, waits until it is in flight.
proc slab_held_read {coll {cmd SLMGET}} {
    set rd [valkey_deferring_client]
    set before [slab_stat slab_reads_dispatched]
    $rd $cmd $coll {*}[slab_keys 100]
    wait_for_condition 100 10 {[slab_stat slab_reads_dispatched] > $before} else {
        fail "the read was not dispatched to a query thread"
    }
    return $rd
}

start_server {tags {slab "external:skip"} overrides {slab-query-threads 2}} {
    test {Large reads run on query threads and match the inline result} {
        r del c
        r slcreate c FLUSH_ENTRIES 500 BLOCK_BYTES 4096
        slab_fill c 3000 a
        r slflush c WAIT 0
        slab_fill c 200 b
        r sldel c k000005
        set before [slab_stat slab_reads_dispatched]
        set async [r slmget c {*}[slab_keys 300] missing]
        assert {[slab_stat slab_reads_dispatched] > $before}
        # Below the threshold the same keys are read inline.
        set inline {}
        foreach k [slab_keys 300] { lappend inline [r slget c $k] }
        lappend inline {}
        assert_equal $inline $async
        assert_equal {} [lindex $async 5]
        assert_equal v6:b [lindex $async 6]
    }

    test {SLSCAN and SLCOUNT EXACT run on query threads} {
        set before [slab_stat slab_reads_dispatched]
        set cursor 0
        set n 0
        while 1 {
            set res [r slscan c $cursor COUNT 250]
            incr n [expr {[llength [lindex $res 1]] / 2}]
            set cursor [lindex $res 0]
            if {$cursor eq "0"} break
        }
        assert_equal 2999 $n
        assert_equal 2999 [r slcount c EXACT]
        assert_equal OK [r debug slab verify c]
        assert {[slab_stat slab_reads_dispatched] > $before + 3}
    }

    test {A held read answers the content at dispatch across writes, MULTI and DEL} {
        r flushall
        slab_fill c 100 old
        r debug slab hold-query
        set rd [slab_held_read c]
        r multi
        r slmset c k000001 new1
        r sldel c k000002
        r exec
        slab_fill c 100 newer
        r del c
        r debug slab release-query
        set reply [$rd read]
        assert_equal v1:old [lindex $reply 1]
        assert_equal v2:old [lindex $reply 2]
        assert_equal v99:old [lindex $reply 99]
        $rd close
    } {0} {needs:debug}

    test {A held read answers for the collection its key named across RENAME, MOVE and SWAPDB} {
        r flushall
        slab_fill c 100 old
        r debug slab hold-query
        set rd [slab_held_read c]
        r rename c c2
        r slmset c2 k000001 renamed
        r move c2 10
        r swapdb 9 10
        r slmset c2 k000001 swapped
        r flushall
        r debug slab release-query
        set reply [$rd read]
        assert_equal v1:old [lindex $reply 1]
        $rd close
    } {0} {needs:debug singledb:skip}

    test {A read's reply precedes the invalidation of a later write (RESP3 tracking)} {
        r flushall
        slab_fill c 100 old
        set rd [valkey_deferring_client]
        $rd hello 3
        $rd read
        $rd client tracking on
        $rd read
        r debug slab hold-query
        set before [slab_stat slab_reads_dispatched]
        $rd slmget c {*}[slab_keys 100]
        wait_for_condition 100 10 {[slab_stat slab_reads_dispatched] > $before} else {
            fail "a tracking connection's read did not go to a query thread"
        }
        # The write invalidates the key the read is tracking.
        r slmset c k000001 new
        r debug slab release-query
        set first [$rd read]
        assert_equal v1:old [lindex $first 1]
        set second [$rd read]
        assert_match {*invalidate*c*} $second
        $rd close
    } {0} {needs:debug resp3}

    test {A timeout left by an earlier blocking command does not apply to slab reads} {
        r flushall
        slab_fill c 100 t
        set rd [valkey_deferring_client]
        $rd blpop nosuchlist 0.01
        assert_equal {} [$rd read]
        r debug slab hold-query
        $rd slmget c {*}[slab_keys 100]
        # Past the old deadline the read is still waiting, not timed out.
        after 100
        assert_equal PONG [r ping]
        r debug slab release-query
        assert_equal v5:t [lindex [$rd read] 5]
        $rd slflush c WAIT 0
        assert_match {[0-9]*} [$rd read]
        $rd close
    } {0} {needs:debug}

    test {Tracking with REDIRECT reads inline (its invalidations use another connection)} {
        r flushall
        slab_fill c 100 x
        set redir [valkey_deferring_client]
        $redir client id
        set redir_id [$redir read]
        set rd [valkey_deferring_client]
        $rd client tracking on redirect $redir_id
        $rd read
        set before [slab_stat slab_reads_dispatched]
        set context [slab_stat slab_reads_inline_context]
        $rd slmget c {*}[slab_keys 100]
        assert_equal v1:x [lindex [$rd read] 1]
        assert_equal $before [slab_stat slab_reads_dispatched]
        assert {[slab_stat slab_reads_inline_context] > $context}
        $rd close
        $redir close
    } {0}

    test {Killing a client with a read in flight drops its reply} {
        r flushall
        slab_fill c 100 x
        r debug slab hold-query
        set rd [slab_held_read c]
        $rd close
        wait_for_condition 100 10 {[s blocked_clients] == 0} else { fail "client still blocked" }
        set orphaned [slab_stat slab_reads_orphaned]
        r debug slab release-query
        wait_for_condition 100 10 {[slab_stat slab_reads_orphaned] > $orphaned} else {
            fail "the orphaned reply was not dropped"
        }
        assert_equal 0 [slab_stat slab_reads_inflight]
        assert_equal PONG [r ping]
    } {} {needs:debug}

    test {Reads in MULTI and scripts run inline with the same replies} {
        r flushall
        slab_fill c 300 m
        set before [slab_stat slab_reads_inline_context]
        r multi
        r slmget c {*}[slab_keys 100]
        r slcount c EXACT
        r slscan c 0 COUNT 5
        set res [r exec]
        assert_equal v7:m [lindex [lindex $res 0] 7]
        assert_equal 300 [lindex $res 1]
        assert_equal [r slscan c 0 COUNT 5] [lindex $res 2]
        r eval {return redis.call('slmget', KEYS[1], unpack(ARGV))} 1 c {*}[slab_keys 100]
        assert_equal 300 [r eval {return redis.call('slcount', KEYS[1], 'EXACT')} 1 c]
        assert {[slab_stat slab_reads_inline_context] >= $before + 4}
    }

    test {A reply from a query thread counts as the command's output (COMMANDLOG large-reply)} {
        r flushall
        slab_fill c 100 x
        r config set commandlog-reply-larger-than 500
        set before [slab_stat slab_reads_dispatched]
        r commandlog reset large-reply
        r slmget c {*}[slab_keys 100]
        set entry [lindex [r commandlog get 1 large-reply] 0]
        assert {[slab_stat slab_reads_dispatched] > $before}
        assert_equal slmget [string tolower [lindex $entry 3 0]]
        # *100, then $<len> and the value for each of the 100 entries
        set want [string length "*100\r\n"]
        for {set j 0} {$j < 100} {incr j} {
            incr want [string length "\$[string length v$j:x]\r\nv$j:x\r\n"]
        }
        assert_equal $want [lindex $entry 2]
        r config set commandlog-reply-larger-than 1048576
    }

    test {RESP3 replies use RESP3 nulls} {
        r hello 3
        set res [r slmget c {*}[slab_keys 70] missing]
        assert_equal {} [lindex $res 70]
        r hello 2
        r exists c
    } {1} {resp3}

    test {Concurrent batch reads never see part of a write} {
        r flushall
        # Every write sets all 200 keys to one version; a batch must see one version.
        set keys [slab_keys 200]
        proc write_version {v keys} {
            set args {}
            foreach k $keys { lappend args $k $v }
            r slmset c {*}$args
        }
        write_version 0 $keys
        set readers {}
        for {set i 0} {$i < 4} {incr i} {
            set rd [valkey_deferring_client]
            lappend readers $rd
        }
        for {set v 1} {$v <= 60} {incr v} {
            write_version $v $keys
            if {$v % 10 == 0} { r slflush c }
            foreach rd $readers { $rd slmget c {*}$keys }
            foreach rd $readers {
                set reply [$rd read]
                set seen [lsort -unique $reply]
                assert_equal 1 [llength $seen]
                assert {$seen >= $v}
            }
        }
        foreach rd $readers { $rd close }
    }
}

start_server {tags {slab "external:skip"} overrides {slab-query-threads 3 io-threads 4}} {
    test {Many concurrent reads through IO threads stay exact} {
        r slcreate c FLUSH_ENTRIES 1000 BLOCK_BYTES 8192
        slab_fill c 5000 io
        r slflush c WAIT 0
        set clients {}
        for {set i 0} {$i < 8} {incr i} { lappend clients [valkey_deferring_client] }
        for {set round 0} {$round < 20} {incr round} {
            set i 0
            foreach rd $clients {
                set start [expr {($round * 97 + $i * 311) % 4800}]
                set keys {}
                for {set j 0} {$j < 150} {incr j} { lappend keys [format k%06d [expr {$start + $j}]] }
                $rd slmget c {*}$keys
                incr i
            }
            set i 0
            foreach rd $clients {
                set start [expr {($round * 97 + $i * 311) % 4800}]
                set reply [$rd read]
                assert_equal "v[expr {$start + 149}]:io" [lindex $reply 149]
                incr i
            }
        }
        foreach rd $clients { $rd close }
    }
}
