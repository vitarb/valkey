# Slab collections: RDB, DUMP/RESTORE, AOF and replication.

proc slab_fill {coll n tag {prefix k}} {
    for {set i 0} {$i < $n} {incr i 100} {
        set args {}
        for {set j $i} {$j < $i + 100 && $j < $n} {incr j} {
            lappend args [format "%s%06d" $prefix $j] "v$j:$tag"
        }
        r slmset $coll {*}$args
    }
}

proc slab_info_field {coll field} {
    return [dict get [r slinfo $coll FULL] $field]
}

# A collection with every layer populated: base blocks, leaf deltas with tombstones,
# and pending writes in the active memtable.
proc slab_build_layers {coll} {
    r del $coll
    r slcreate $coll FLUSH_ENTRIES 1000 BLOCK_BYTES 4096 AUTO_COMPACT no
    slab_fill $coll 6000 base
    r slflush $coll WAIT 0
    slab_fill $coll 400 delta
    r sldel $coll k000003 k000100 k005999
    r slflush $coll WAIT 0
    slab_fill $coll 50 pending
    r sldel $coll k000004
    r slmset $coll "" empty-key [binary format H* 00ff] [binary format H* ff00]
}

start_server {tags {slab "external:skip"} overrides {forkless-infrastructure-enabled yes}} {
    test {DEBUG RELOAD keeps every layer, the options and the digest} {
        slab_build_layers c
        assert {[slab_info_field c delta_entries] > 0}
        assert {[slab_info_field c pending_writes] > 0}
        set digest [r debug digest-value c]
        set count [r slcount c EXACT]
        r debug reload
        assert_equal $digest [r debug digest-value c]
        assert_equal $count [r slcount c EXACT]
        assert_equal OK [r debug slab verify c]
        assert_equal 4096 [slab_info_field c block_bytes]
        assert_equal 1000 [slab_info_field c flush_entries]
        assert_equal 0 [slab_info_field c auto_compact]
        assert_equal v3:pending [r slget c k000003]
        assert_equal {} [r slget c k000100]
        assert_equal {} [r slget c k000004]
        assert_equal v5:pending [r slget c k000005]
        assert_equal empty-key [r slget c ""]
        assert_equal [binary format H* ff00] [r slget c [binary format H* 00ff]]
        # Writes after the load go on top of the loaded state.
        r slmset c k000005 after-load
        r slflush c WAIT 0
        assert_equal after-load [r slget c k000005]
        assert_equal OK [r debug slab verify c]
    } {} {needs:debug needs:reload}

    test {More than 64 frozen memtables are scanned and saved} {
        r flushall
        r debug slab max-frozen-memtables 200
        r debug slab pause-maintenance
        for {set i 0} {$i < 70} {incr i} {
            r slmset c k$i v$i
            r slflush c
        }
        assert {[dict get [r slinfo c] frozen_memtables] >= 70}
        assert_equal 70 [r slcount c EXACT]
        assert_equal 140 [llength [lindex [r slscan c 0 COUNT 1000] 1]]
        r debug slab resume-maintenance
        r debug reload
        assert_equal 70 [r slcount c EXACT]
        assert_equal v0 [r slget c k0]
        r debug slab max-frozen-memtables 0
    } {OK} {needs:debug needs:reload slab:threads}

    test {An empty collection survives a reload} {
        r del e
        r slcreate e FLUSH_ENTRIES 77
        r debug reload
        assert_equal slab [r type e]
        assert_equal 0 [r slcount e EXACT]
        assert_equal 77 [slab_info_field e flush_entries]
    } {} {needs:debug needs:reload}

    foreach method {fork forkless} {
        test "BGSAVE ($method) writes a loadable collection" {
            slab_build_layers c
            set digest [r debug digest-value c]
            r config set bgsave-default-method $method
            r bgsave
            waitForBgsave r
            assert_equal $method [s rdb_last_bgsave_type]
            assert_equal ok [s rdb_last_bgsave_status]
            r debug reload nosave
            assert_equal $digest [r debug digest-value c]
            r config set bgsave-default-method fork
            assert_equal OK [r debug slab verify c]
        } {} {needs:debug needs:reload}
    }

    test {A fork past a stalled maintenance section fails the save visibly; the next one succeeds} {
        slab_build_layers c
        set digest [r debug digest-value c]
        set timeouts [getInfoProperty [r info debug] slab_fork_prepare_timeouts]
        # The flush of the pending writes stops in its first section, as a maintenance
        # thread descheduled past the fork's limit would.
        r debug slab hold-sections
        r slflush c
        wait_for_condition 100 20 {[getInfoProperty [r info debug] slab_sections_held] >= 1} else {
            fail "no maintenance section was held"
        }
        r config set bgsave-default-method fork
        r bgsave
        waitForBgsave r
        assert_equal err [s rdb_last_bgsave_status]
        assert {[getInfoProperty [r info debug] slab_fork_prepare_timeouts] > $timeouts}
        r debug slab release-sections
        r slflush c WAIT 0
        r bgsave
        waitForBgsave r
        assert_equal ok [s rdb_last_bgsave_status]
        r debug reload nosave
        assert_equal $digest [r debug digest-value c]
        assert_equal OK [r debug slab verify c]
    } {} {needs:debug needs:reload slab:threads}

    test {A forkless BGSAVE saves the content at hand-out while writes and flushes follow} {
        slab_build_layers c
        r config set bgsave-default-method forkless
        r config set rdb-key-save-delay 200000
        r set other 1
        r bgsave
        wait_for_condition 100 10 {[s rdb_bgsave_in_progress] eq 1} else { fail "bgsave did not start" }
        # The collection is held for the iteration client, so writes to it wait; a flush
        # of its pending writes still installs a new View while it is in use.
        r slflush c WAIT 0
        r config set rdb-key-save-delay 0
        waitForBgsave r
        assert_equal ok [s rdb_last_bgsave_status]
        set digest [r debug digest-value c]
        r config set bgsave-default-method fork
        r debug reload nosave
        assert_equal $digest [r debug digest-value c]
    } {} {needs:debug needs:reload}

    test {DUMP and RESTORE copy a collection} {
        slab_build_layers c
        set payload [r dump c]
        r del d
        r restore d 0 $payload
        assert_equal [r debug digest-value c] [r debug digest-value d]
        assert_equal OK [r debug slab verify d]
        assert_equal 4096 [slab_info_field d block_bytes]
    } {} {needs:debug}

    test {RESTORE rejects corrupted slab payloads without crashing} {
        r del c
        r slcreate c FLUSH_ENTRIES 200 BLOCK_BYTES 1024
        slab_fill c 800 x
        r slflush c WAIT 0
        slab_fill c 100 y
        r sldel c k000001
        r slflush c WAIT 0
        slab_fill c 10 z
        set payload [r dump c]
        set len [string length $payload]
        r debug set-skip-checksum-validation 1
        # Truncations fail.
        foreach cut [list 1 10 100 [expr {$len / 2}] [expr {$len - 11}]] {
            catch {r restore t 0 [string range $payload 0 $cut] replace} err
            assert_match {*ERR*} $err
        }
        # Single-byte flips (the trailer excluded) either fail or load a collection that
        # passes its own verification.
        set loaded 0
        for {set i 0} {$i < 300} {incr i} {
            set pos [expr {int(rand() * ($len - 10))}]
            set b [scan [string index $payload $pos] %c]
            set flipped [string replace $payload $pos $pos [format %c [expr {$b ^ (1 << ($i % 8))}]]]
            if {[catch {r restore t 0 $flipped replace} err]} {
                continue
            }
            incr loaded
            assert_equal OK [r debug slab verify t]
            r slmget t k000001 k000500 k000799
        }
        r debug set-skip-checksum-validation 0
        assert_equal PONG [r ping]
    } {} {needs:debug}

    foreach preamble {no yes} {
        test "AOF rewrite (preamble $preamble) restores the collection and its options" {
            r flushall
            r config set aof-use-rdb-preamble $preamble
            r config set appendonly yes
            waitForBgrewriteaof r
            slab_build_layers c
            r slcreate empty
            set digest [r debug digest-value c]
            r bgrewriteaof
            waitForBgrewriteaof r
            assert_equal ok [s aof_last_bgrewrite_status]
            r slmset c k000007 after-rewrite
            r debug loadaof
            assert_equal after-rewrite [r slget c k000007]
            r slmset c k000007 v7:pending
            assert_equal $digest [r debug digest-value c]
            assert_equal slab [r type empty]
            assert_equal 4096 [slab_info_field c block_bytes]
            r config set appendonly no
            assert_equal OK [r debug slab verify c]
        } {} {needs:debug}
    }

    test {valkey-check-rdb accepts a file with slab collections} {
        slab_build_layers c
        r save
        set dir [lindex [r config get dir] 1]
        set out [exec $::VALKEY_CHECK_RDB_BIN [file join $dir [lindex [r config get dbfilename] 1]] --stats --format info]
        assert_match {*RDB looks OK*} $out
        assert {[regexp {db\.9\.type\.slab\.keys\.total:([0-9]+)} $out -> n]}
        assert {$n >= 1}
    } {} {needs:save external:skip}
}

start_server {tags {slab repl "external:skip"}} {
    set primary [srv 0 client]
    start_server {} {
        set replica [srv 0 client]
        test {A replica full-syncs slab collections} {
            $primary flushall
            $primary slcreate c FLUSH_ENTRIES 500 BLOCK_BYTES 2048
            for {set i 0} {$i < 3000} {incr i 100} {
                set args {}
                for {set j $i} {$j < $i + 100} {incr j} { lappend args [format k%06d $j] v$j }
                $primary slmset c {*}$args
            }
            $primary slflush c WAIT 0
            $replica replicaof [srv -1 host] [srv -1 port]
            wait_for_sync $replica
            assert_equal [$primary debug digest-value c] [$replica debug digest-value c]
        } {} {needs:debug}

        test {Writes and creates, implicit or with options, replicate} {
            $primary slmset c k000001 changed
            $primary sldel c k000002
            $primary slmset implicit a 1
            $primary slcreate explicit NX BLOCK_BYTES 8192 AUTO_COMPACT no
            wait_for_ofs_sync $primary $replica
            assert_equal changed [$replica slget c k000001]
            assert_equal {} [$replica slget c k000002]
            assert_equal 1 [$replica slget implicit a]
            assert_equal 0 [dict get [$replica slinfo explicit FULL] auto_compact]
            assert_equal 8192 [dict get [$replica slinfo explicit FULL] block_bytes]
            assert_equal [$primary debug digest] [$replica debug digest]
        } {} {needs:debug}

        test {A replica applies writes past its own client limits} {
            $replica debug slab max-pending-bytes 1
            $primary slmset c x1 1 x2 2 x3 3
            wait_for_ofs_sync $primary $replica
            assert_equal 3 [$replica slget c x3]
            $replica debug slab max-pending-bytes 0
        } {OK} {needs:debug}
    }
}
