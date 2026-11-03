# Slab reads on the IO threads: a read takes effect at its command's position, whatever
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

# Sends a large read from deferring client rd until one leaves the main thread. A read
# that finds no IO thread active runs inline (with TLS, a command can run from pending
# data while the always-active threads are parked before sleep): its reply is read and
# the read sent again.
proc slab_send_offloaded {rd args} {
    for {set try 0} {$try < 20} {incr try} {
        set before [slab_stat slab_reads_dispatched]
        set inline [slab_stat slab_reads_inline_no_thread]
        $rd {*}$args
        wait_for_condition 100 10 {
            [slab_stat slab_reads_dispatched] > $before || [slab_stat slab_reads_inline_no_thread] > $inline
        } else {
            fail "the read neither left the main thread nor ran inline"
        }
        if {[slab_stat slab_reads_dispatched] > $before} return
        $rd read
    }
    fail "the read never left the main thread"
}

# Sends a held 100-key read from a new deferring client, waits until it is in flight.
proc slab_held_read {coll {cmd SLMGET}} {
    set rd [valkey_deferring_client]
    slab_send_offloaded $rd $cmd $coll {*}[slab_keys 100]
    return $rd
}

# Every large read goes to the IO threads (always active).
foreach slab_async_overrides {{io-threads 3 io-threads-always-active yes}} {
start_server [list tags {slab "external:skip"} overrides $slab_async_overrides] {
    test {Large reads run off the main thread and match the inline result} {
        r del c
        r slcreate c FLUSH_ENTRIES 500 BLOCK_BYTES 4096
        slab_fill c 3000 a
        r slflush c WAIT 0
        slab_fill c 200 b
        r sldel c k000005
        set rd [valkey_deferring_client]
        slab_send_offloaded $rd slmget c {*}[slab_keys 300] missing
        set async [$rd read]
        $rd close
        # Below the threshold the same keys are read inline.
        set inline {}
        foreach k [slab_keys 300] { lappend inline [r slget c $k] }
        lappend inline {}
        assert_equal $inline $async
        assert_equal {} [lindex $async 5]
        assert_equal v6:b [lindex $async 6]
    }

    test {SLSCAN and SLCOUNT EXACT run off the main thread} {
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
        # A page of several runner steps matches the same page read inline.
        set res [r slscan c 0 COUNT 1500]
        assert {[llength [lindex $res 1]] > 2000}
        r multi
        r slscan c 0 COUNT 1500
        assert_equal [list $res] [r exec]
        assert_equal OK [r debug slab verify c]
        assert {[slab_stat slab_reads_dispatched] > $before + 3}
    }

    test {A held read answers the content at dispatch across writes, MULTI and DEL} {
        r flushall
        slab_fill c 100 old
        r debug slab hold-reads
        set rd [slab_held_read c]
        r multi
        r slmset c k000001 new1
        r sldel c k000002
        r exec
        slab_fill c 100 newer
        r del c
        r debug slab release-reads
        set reply [$rd read]
        assert_equal v1:old [lindex $reply 1]
        assert_equal v2:old [lindex $reply 2]
        assert_equal v99:old [lindex $reply 99]
        $rd close
    } {0} {needs:debug}

    test {A held read answers for the collection its key named across RENAME, MOVE and SWAPDB} {
        r flushall
        slab_fill c 100 old
        r debug slab hold-reads
        set rd [slab_held_read c]
        r rename c c2
        r slmset c2 k000001 renamed
        r move c2 10
        r swapdb 9 10
        r slmset c2 k000001 swapped
        r flushall
        r debug slab release-reads
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
        r debug slab hold-reads
        slab_send_offloaded $rd slmget c {*}[slab_keys 100]
        # The write invalidates the key the read is tracking.
        r slmset c k000001 new
        r debug slab release-reads
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
        r debug slab hold-reads
        $rd slmget c {*}[slab_keys 100]
        # Past the old deadline the read is still waiting, not timed out.
        after 100
        assert_equal PONG [r ping]
        r debug slab release-reads
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
        r debug slab hold-reads
        set rd [slab_held_read c]
        $rd close
        wait_for_condition 100 10 {[s blocked_clients] == 0} else { fail "client still blocked" }
        set orphaned [slab_stat slab_reads_orphaned]
        r debug slab release-reads
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

    test {A reply built off the main thread counts as the command's output (COMMANDLOG large-reply)} {
        r flushall
        slab_fill c 100 x
        r config set commandlog-reply-larger-than 500
        set rd [valkey_deferring_client]
        r commandlog reset large-reply
        slab_send_offloaded $rd slmget c {*}[slab_keys 100]
        $rd read
        $rd close
        # The helper's INFO replies are logged too: take the read's entry.
        set entry {}
        foreach e [r commandlog get -1 large-reply] {
            if {[string tolower [lindex $e 3 0]] eq "slmget"} { set entry $e; break }
        }
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

    test {Reads over unflushed frozen memtables match the inline result} {
        r flushall
        r slcreate c FLUSH_ENTRIES 300
        r debug slab pause-maintenance
        # Each fill freezes a memtable maintenance does not flush: a runner sorts them one
        # step each before its iteration.
        for {set i 0} {$i < 3} {incr i} {
            slab_fill c 400 m$i
            r slflush c
        }
        slab_fill c 50 active
        set rd [valkey_deferring_client]
        slab_send_offloaded $rd slscan c 0 COUNT 1000
        set scan [$rd read]
        slab_send_offloaded $rd slcount c EXACT
        set count [$rd read]
        $rd close
        r multi
        r slscan c 0 COUNT 1000
        r slcount c EXACT
        assert_equal [list $scan $count] [r exec]
        assert_equal 400 $count
        r debug slab resume-maintenance
    } {OK} {needs:debug}

    test {A read of many chunks keeps key order} {
        r flushall
        slab_fill c 2000 o
        r sldel c k000500
        set keys [lreverse [slab_keys 2000]]
        set reply [r slmget c {*}$keys]
        assert_equal 2000 [llength $reply]
        assert_equal v1999:o [lindex $reply 0]
        assert_equal {} [lindex $reply 1499]
        assert_equal v0:o [lindex $reply end]
    }
}
}

start_server {tags {slab "external:skip"} overrides {io-threads 4}} {
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

    test {Large reads from one connection activate the IO threads} {
        # Few connections give the IO threads too little network work to start; a large
        # read that finds no active thread runs inline and activates one for the next.
        wait_for_condition 100 50 {[s io_threads_active] == 0} else {
            fail "the IO threads did not park"
        }
        set before [slab_stat slab_reads_dispatched]
        for {set i 0} {$i < 20} {incr i} {
            assert_equal v999:io [lindex [r slmget c {*}[slab_keys 1000]] 999]
        }
        assert {[slab_stat slab_reads_dispatched] - $before >= 15}
    }

    test {Changing io-threads with a held read neither hangs nor loses it} {
        # A held read is not handed to the IO threads, so their drain doesn't wait for it;
        # released after they are gone, it runs and completes on the main thread.
        r config set io-threads 4
        set rd [valkey_deferring_client]
        r debug slab hold-reads
        slab_send_offloaded $rd slmget c {*}[slab_keys 1000]
        r config set io-threads 1
        assert_equal PONG [r ping]
        r debug slab release-reads
        assert_equal v999:io [lindex [$rd read] 999]
        assert_equal 0 [slab_stat slab_reads_inflight]
        $rd close
        r config set io-threads 4
    } {OK} {needs:debug}

    test {Changing io-threads with reads in flight answers them all} {
        set clients {}
        for {set i 0} {$i < 8} {incr i} { lappend clients [valkey_deferring_client] }
        foreach rd $clients { $rd slmget c {*}[slab_keys 1000] }
        r config set io-threads 2
        foreach rd $clients { assert_equal v999:io [lindex [$rd read] 999] }
        foreach rd $clients { $rd slmget c {*}[slab_keys 1000] }
        r config set io-threads 1
        foreach rd $clients { assert_equal v999:io [lindex [$rd read] 999] }
        assert_equal 0 [slab_stat slab_reads_inflight]
        foreach rd $clients { $rd close }
    }
}
