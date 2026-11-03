# Slab collections: commands, keyspace integration and the consistency rules that
# hold without background threads.

# Large reads go to query threads here (their default follows io-threads, so it is 0
# with one IO thread); slab-inline.tcl runs this file again with no slab threads, and
# its overrides come later, so they win.
set slab_saved_overrides_slab $::global_overrides
set ::global_overrides [concat {slab-query-threads 2} $::global_overrides]

# Writes n entries "k<i>" -> "v<i>:<tag>" in batches of 100.
proc slab_fill {coll n tag {prefix k}} {
    for {set i 0} {$i < $n} {incr i 100} {
        set args {}
        for {set j $i} {$j < $i + 100 && $j < $n} {incr j} {
            lappend args [format "%s%06d" $prefix $j] "v$j:$tag"
        }
        r slmset $coll {*}$args
    }
}

# All live entries via SLSCAN with the given COUNT, as a flat list.
proc slab_scan_all {coll {count 100} args} {
    set cursor 0
    set out {}
    while 1 {
        set res [r slscan $coll $cursor COUNT $count {*}$args]
        lappend out {*}[lindex $res 1]
        set cursor [lindex $res 0]
        if {$cursor eq "0"} break
    }
    return $out
}

proc slab_stat {field} {
    return [getInfoProperty [r info slab debug] $field]
}

proc slab_info_field {coll field} {
    set info [r slinfo $coll FULL]
    return [dict get $info $field]
}

start_server {tags {slab}} {
    test {SLMSET creates a collection and SLGET reads it back} {
        r del c
        assert_equal 2 [r slmset c a 1 b 2]
        assert_equal slab [r type c]
        assert_equal slabtree [r object encoding c]
        assert_equal 1 [r slget c a]
        assert_equal 2 [r slget c b]
        assert_equal {} [r slget c missing]
        assert_equal {} [r slget nosuch a]
        assert_equal {1 {} 2} [r slmget c a x b]
        assert_equal {{} {}} [r slmget nosuch a b]
    }

    test {Keys and values are binary safe, including empty ones} {
        r del c
        set k [binary format H* 00ff00]
        set v [binary format H* ff0001]
        r slmset c $k $v "" empty emptyval ""
        assert_equal $v [r slget c $k]
        assert_equal empty [r slget c ""]
        assert_equal "" [r slget c emptyval]
        r slflush c WAIT 0
        assert_equal $v [r slget c $k]
        assert_equal empty [r slget c ""]
    }

    test {A repeated key in one SLMSET is last-wins} {
        r del c
        r slmset c k first k second
        assert_equal second [r slget c k]
    }

    test {SLDEL writes tombstones that hide older values in every layer} {
        r del c
        r slcreate c FLUSH_ENTRIES 100
        slab_fill c 1000 a
        r slflush c WAIT 0
        assert_equal 3 [r sldel c k000001 k000500 nosuch]
        assert_equal {} [r slget c k000001]
        assert_equal {} [r slget c k000500]
        assert_equal v2:a [r slget c k000002]
        r slflush c WAIT 0
        r slcompact c FULL WAIT 0
        assert_equal {} [r slget c k000001]
        assert_equal 998 [r slcount c EXACT]
        assert_equal 0 [r sldel nosuch a]
    }

    test {SLCREATE options, NX and errors} {
        r del c
        assert_equal 1 [r slcreate c BLOCK_BYTES 4096 FLUSH_INTERVAL_MS 500 AUTO_COMPACT no]
        assert_equal 4096 [slab_info_field c block_bytes]
        assert_equal 500 [slab_info_field c flush_interval_ms]
        assert_equal 0 [slab_info_field c auto_compact]
        assert_equal 0 [r slcreate c NX]
        assert_error {*already exists*} {r slcreate c}
        assert_error {*unknown slab option*} {r slcreate d NOPE 1}
        assert_error {*unknown slab option*} {r slcreate d KEY_RESTART 4}
        assert_error {*given twice*} {r slcreate d FLUSH_ENTRIES 4 flush_entries 5}
        assert_error {*invalid value*} {r slcreate d AUTO_COMPACT maybe}
        assert_error {*BLOCK_BYTES*} {r slcreate d BLOCK_BYTES 10}
        assert_equal 0 [r exists d]
        r slalter c AUTO_COMPACT yes FLUSH_ENTRIES 50
        assert_equal 1 [slab_info_field c auto_compact]
        assert_equal 50 [slab_info_field c flush_entries]
        # The short form leaves out the layout, the counters and the options.
        assert_equal 0 [dict exists [r slinfo c] block_bytes]
        assert_equal 1 [dict exists [r slinfo c] entries]
        assert_error {*syntax*} {r slinfo c BRIEF}
        assert_error {*no such slab collection*} {r slalter nosuch FLUSH_ENTRIES 1}
    }

    test {Slab commands reject other types and others reject slab} {
        r del s c
        r set s x
        assert_error {WRONGTYPE*} {r slmset s a 1}
        assert_error {WRONGTYPE*} {r slget s a}
        r slmset c a 1
        assert_error {WRONGTYPE*} {r get c}
        assert_error {WRONGTYPE*} {r hget c a}
    }

    test {Write size limits apply to client writes} {
        r del c
        assert_error {*slab key exceeds 1024 bytes*} {r slmset c [string repeat k 1025] v}
        assert_error {*slab value exceeds 1048576 bytes*} {r slmset c k [string repeat v 1048577]}
        set args {}
        for {set i 0} {$i <= 10000} {incr i} { lappend args $i v }
        assert_error {*batch of 10001 entries exceeds 10000*} {r slmset c {*}$args}
        assert_equal 0 [r exists c]
    }

    test {Reads stay correct across memtables, flushes and compactions} {
        r del c
        r slcreate c FLUSH_ENTRIES 500 BLOCK_BYTES 4096
        set model [dict create]
        for {set round 0} {$round < 6} {incr round} {
            set args {}
            for {set i 0} {$i < 1500} {incr i} {
                set k [format "key:%05d" [expr {($i * 7 + $round * 131) % 3000}]]
                lappend args $k "r$round:$i"
                dict set model $k "r$round:$i"
            }
            for {set i 0} {$i < [llength $args]} {incr i 200} {
                r slmset c {*}[lrange $args $i [expr {$i + 199}]]
            }
            set dels {}
            for {set i 0} {$i < 40} {incr i} {
                set k [format "key:%05d" [expr {($i * 61 + $round) % 3000}]]
                lappend dels $k
                dict unset model $k
            }
            r sldel c {*}$dels
            if {$round % 2} { r slflush c }
            if {$round == 4} { r slcompact c FULL }
        }
        assert_equal OK [r debug slab verify c]
        set keys {}
        for {set i 0} {$i < 3000} {incr i} { lappend keys [format "key:%05d" $i] }
        set got [r slmget c {*}$keys]
        for {set i 0} {$i < 3000} {incr i} {
            set k [lindex $keys $i]
            set want [expr {[dict exists $model $k] ? [dict get $model $k] : ""}]
            assert_equal $want [lindex $got $i]
        }
        assert_equal [dict size $model] [r slcount c EXACT]
        set scanned [dict create {*}[slab_scan_all c 37]]
        assert_equal [dict size $model] [dict size $scanned]
        dict for {k v} $model { assert_equal $v [dict get $scanned $k] }
    }

    test {SLSCAN returns every entry once, in key order, with PREFIX and NOVALUES} {
        r del c
        r slcreate c FLUSH_ENTRIES 300 BLOCK_BYTES 2048
        slab_fill c 2000 x
        slab_fill c 500 y p
        r sldel c k000010 k000011
        set all [slab_scan_all c 64]
        assert_equal [expr {(2000 - 2 + 500) * 2}] [llength $all]
        set keys {}
        foreach {k v} $all { lappend keys $k }
        assert_equal $keys [lsort $keys]
        set p [slab_scan_all c 50 PREFIX p]
        assert_equal 1000 [llength $p]
        assert_equal [format p%06d 0] [lindex $p 0]
        set nv [slab_scan_all c 1000 NOVALUES]
        assert_equal 2498 [llength $nv]
        assert_equal {0 {}} [r slscan nosuch 0]
        assert_error {*invalid slab scan cursor*} {r slscan c bogus}
    }

    test {SLCOUNT estimates without EXACT and counts with it} {
        r del c
        slab_fill c 1000 a
        assert_equal 1000 [r slcount c]
        assert_equal 1000 [r slcount c EXACT]
        assert_equal 0 [r slcount nosuch]
    }

    test {SLINFO reports structure and settles after an explicit flush} {
        r del c
        r slcreate c FLUSH_ENTRIES 1000
        slab_fill c 5000 a
        r slflush c WAIT 0
        assert_equal 0 [slab_info_field c frozen_memtables]
        assert_equal 0 [slab_info_field c flush_inflight]
        assert_equal 0 [slab_info_field c maintenance_queue_depth]
        assert_equal 5000 [slab_info_field c entries]
        assert {[slab_info_field c blocks] >= 1}
        assert_error {*no such slab collection*} {r slinfo nosuch}
    }

    test {MULTI and Lua see slab writes atomically} {
        r del c
        r multi
        r slmset c a 1
        r slmset c b 2
        r slmget c a b
        set res [r exec]
        assert_equal {1 1 {1 2}} $res
        assert_equal {1 2} [r eval {return redis.call('slmget', KEYS[1], 'a', 'b')} 1 c]
        r eval {redis.call('slmset', KEYS[1], 'a', 'lua')} 1 c
        assert_equal lua [r slget c a]
    }

    test {WATCH is invalidated by a slab write} {
        r del c
        r slmset c a 1
        r watch c
        set rd [valkey_client]
        $rd slmset c a 2
        $rd close
        r multi
        r slget c a
        assert_equal {} [r exec]
    }

    test {COPY makes an independent collection} {
        r del c d
        r slcreate c FLUSH_ENTRIES 200
        slab_fill c 1000 a
        r slflush c WAIT 0
        r slmset c k000001 changed-before-copy
        assert_equal 1 [r copy c d]
        r slmset c k000002 src-only
        r slmset d k000003 dst-only
        assert_equal changed-before-copy [r slget d k000001]
        assert_equal v2:a [r slget d k000002]
        assert_equal src-only [r slget c k000002]
        assert_equal v3:a [r slget c k000003]
        assert_equal dst-only [r slget d k000003]
        slab_fill d 1000 b
        r slflush d WAIT 0
        r slcompact d FULL WAIT 0
        assert_equal changed-before-copy [r slget c k000001]
        assert_equal v1:b [r slget d k000001]
        assert_equal OK [r debug slab verify c]
        assert_equal OK [r debug slab verify d]
    }

    test {RENAME, MOVE, SWAPDB, EXPIRE and FLUSHALL act on the whole collection} {
        r flushall
        r select 9
        r slmset c a 1
        r rename c c2
        assert_equal 1 [r slget c2 a]
        r move c2 10
        assert_equal 0 [r exists c2]
        r select 10
        assert_equal 1 [r slget c2 a]
        r swapdb 10 9
        r select 9
        assert_equal 1 [r slget c2 a]
        r pexpire c2 50
        after 100
        assert_equal 0 [r exists c2]
        r slmset c a 1
        r flushall
        assert_equal 0 [r exists c]
    } {} {singledb:skip}

    test {SCAN TYPE slab finds collections} {
        r flushall
        r slmset c1 a 1
        r slmset c2 a 1
        r set s 1
        assert_equal {c1 c2} [lsort [lindex [r scan 0 TYPE slab COUNT 100] 1]]
    }

    test {Keyspace events use the c class} {
        r config set notify-keyspace-events KEc
        set rd [valkey_deferring_client]
        $rd psubscribe __keyevent@*__:*
        $rd read
        r del c
        r slmset c a 1
        r sldel c a
        assert_match {*slcreate*} [$rd read]
        assert_match {*slmset*} [$rd read]
        assert_match {*sldel*} [$rd read]
        $rd close
        r config set notify-keyspace-events ""
    }

    test {DEBUG DIGEST-VALUE depends on content, not on layout} {
        r del a b
        r slcreate a FLUSH_ENTRIES 100 BLOCK_BYTES 1024
        r slcreate b
        slab_fill a 2000 z
        slab_fill b 2000 z
        r slflush a WAIT 0
        r slcompact a FULL WAIT 0
        assert_equal [r debug digest-value a] [r debug digest-value b]
        r slmset b k000005 other
        assert {[r debug digest-value a] ne [r debug digest-value b]}
    } {} {needs:debug}

    test {MEMORY USAGE equals the memory a DEL frees} {
        r flushall
        r config set lazyfree-lazy-user-del no
        # Two identical cycles; the second is measured, so one-time allocations of the
        # first (command latency histograms, table resizes) do not count.
        for {set cycle 0} {$cycle < 2} {incr cycle} {
            r slcreate c FLUSH_ENTRIES 2000
            slab_fill c 20000 m
            r slflush c WAIT 0
            slab_fill c 3000 n
            # The 2000-entry memtable freezes; wait for its flush, or the unit's buffers
            # count in used_memory but not in the collection.
            wait_for_condition 500 10 {
                [slab_info_field c flush_inflight] == 0 && [slab_info_field c frozen_memtables] == 0 &&
                [slab_info_field c compaction_inflight] == 0
            } else { fail "collection did not settle" }
            set usage [r memory usage c]
            set before [s used_memory]
            r del c
            # Freed structure goes to the maintenance reclaimer.
            wait_for_condition 100 10 {[slab_stat slab_reclaim_queue_len] == 0} else { fail "reclaimer did not drain" }
            after 20
            set freed [expr {$before - [s used_memory]}]
        }
        r config set lazyfree-lazy-user-del yes
        assert_range $usage [expr {$freed * 0.98}] [expr {$freed * 1.02}]
    } {} {needs:debug}

    test {OBJECT, TYPE and SLVERIFY on a large collection} {
        r del c
        r slcreate c FLUSH_ENTRIES 5000 BLOCK_BYTES 8192
        slab_fill c 30000 big
        r slflush c WAIT 0
        assert_equal OK [r debug slab verify c]
        assert {[slab_info_field c blocks] > 10}
        assert_equal v12345:big [r slget c k012345]
    }
}

start_server {tags {slab "external:skip"}} {
    test {Client writes get -SLABBUSY at the pending cap while maintenance is held} {
        r del c
        r slcreate c FLUSH_ENTRIES 100
        r debug slab max-pending-bytes 200000
        r debug slab pause-maintenance
        set refused 0
        for {set i 0} {$i < 2000} {incr i} {
            if {[catch {r slmset c [format k%06d $i] [string repeat x 1000]} err]} {
                assert_match {SLABBUSY*} $err
                set refused 1
                break
            }
        }
        assert_equal 1 $refused
        assert {[slab_stat slab_writes_rejected_busy] >= 1}
        # Nothing of the refused write was applied.
        assert_equal {} [r slget c [format k%06d $i]]
        r debug slab resume-maintenance
        r debug slab max-pending-bytes 0
        wait_for_condition 100 20 {[dict get [r slinfo c] frozen_memtables] == 0} else {
            fail "maintenance did not catch up"
        }
        r slmset c [format k%06d $i] after
        assert_equal after [r slget c [format k%06d $i]]
    } {} {needs:debug slab:threads}

    test {Too many frozen memtables also refuse client writes} {
        r del c
        r debug slab pause-maintenance
        set refused 0
        # Each SLFLUSH freezes a memtable; with 16 frozen the next write is refused.
        for {set i 0} {$i < 100} {incr i} {
            if {[catch {r slmset c [format k%06d $i] v} err]} {
                assert_match {SLABBUSY*frozen*} $err
                set refused 1
                break
            }
            r slflush c
        }
        assert_equal 1 $refused
        assert_equal 16 [dict get [r slinfo c] frozen_memtables]
        r debug slab resume-maintenance
    } {OK} {needs:debug slab:threads}

    test {SLFLUSH WAIT blocks until the flush is installed and settles the gauges} {
        r del c
        r slcreate c FLUSH_ENTRIES 1000
        r debug slab pause-maintenance
        slab_fill c 3000 w
        set rd [valkey_deferring_client]
        $rd slflush c WAIT 0
        after 100
        assert_equal 1 [s blocked_clients]
        r debug slab resume-maintenance
        assert_equal 0 [$rd read]
        $rd close
        set info [r slinfo c]
        foreach f {frozen_memtables flush_inflight maintenance_queue_depth} {
            assert_equal 0 [dict get $info $f]
        }
    } {} {needs:debug slab:threads}

    test {A client blocked in SLFLUSH WAIT can be killed} {
        r del c
        r slmset c a 1
        r debug slab pause-maintenance
        set rd [valkey_deferring_client]
        $rd client setname waiter
        $rd read
        $rd slflush c WAIT 0
        wait_for_condition 50 10 {[s blocked_clients] == 1} else { fail "not blocked" }
        r client kill skipme yes
        wait_for_condition 50 10 {[s blocked_clients] == 0} else { fail "still blocked" }
        r debug slab resume-maintenance
        $rd close
        assert_equal 0 [slab_stat slab_waiting_clients]
    } {} {needs:debug slab:threads}

    test {SLFLUSH WAIT inside MULTI replies like a timeout when it would block} {
        r del c
        r slmset c a 1
        r debug slab pause-maintenance
        r multi
        r slflush c WAIT 0
        set res [r exec]
        r debug slab resume-maintenance
        assert_equal {{}} $res
    } {} {needs:debug slab:threads}

    test {SLFLUSH WAIT times out, and CLIENT UNBLOCK frees a waiting client} {
        r del c
        r slmset c a 1
        r debug slab pause-maintenance
        assert_equal {} [r slflush c WAIT 50]
        set rd [valkey_deferring_client]
        $rd client id
        set id [$rd read]
        $rd slcompact c FULL WAIT 0
        wait_for_blocked_clients_count 1
        assert_equal 1 [r client unblock $id]
        assert_equal {} [$rd read]
        $rd slflush c WAIT 0
        wait_for_blocked_clients_count 1
        assert_equal 1 [r client unblock $id error]
        assert_error {UNBLOCKED*} {$rd read}
        r debug slab resume-maintenance
        $rd close
        assert_equal 0 [slab_stat slab_waiting_clients]
    } {} {needs:debug slab:threads}

    test {A write larger than the pending cap is refused without SLABBUSY and creates nothing} {
        r del fresh
        r debug slab max-pending-bytes 1000
        set busy [slab_stat slab_writes_rejected_busy]
        # Retrying cannot help, so the error is not the retryable one.
        assert_error {ERR slab write is larger than the pending cap of 1000 bytes*} {r slmset fresh a [string repeat x 5000]}
        assert_equal $busy [slab_stat slab_writes_rejected_busy]
        r debug slab max-pending-bytes 0
        assert_equal 0 [r exists fresh]
    } {} {needs:debug}

    test {Overlapping SLCOMPACT FULL requests all complete} {
        r del c
        r slcreate c FLUSH_ENTRIES 500 BLOCK_BYTES 2048
        slab_fill c 3000 a
        r slflush c WAIT 0
        slab_fill c 3000 b
        r slflush c WAIT 0
        r debug slab pause-maintenance
        r slcompact c FULL
        set rd [valkey_deferring_client]
        $rd slcompact c FULL WAIT 0
        wait_for_blocked_clients_count 1
        r debug slab resume-maintenance
        assert_match {[0-9]*} [$rd read]
        assert_equal 0 [dict get [r slinfo c] delta_entries]
        $rd close
    } {0} {needs:debug slab:threads}

    test {SLCOMPACT compacts leaves over the triggers with AUTO_COMPACT no} {
        r del c
        r slcreate c FLUSH_ENTRIES 500 BLOCK_BYTES 2048 AUTO_COMPACT no
        slab_fill c 2000 a
        r slflush c WAIT 0
        slab_fill c 1500 b
        r slflush c WAIT 0
        assert {[dict get [r slinfo c] delta_entries] > 0}
        assert {[r slcompact c WAIT 0] > 0}
        assert_equal 0 [dict get [r slinfo c] delta_entries]
        assert_equal v7:b [r slget c k000007]
        assert_equal v1999:a [r slget c k001999]
    }

    test {Extra arguments are syntax errors} {
        assert_error {*syntax*} {r slcount c EXACT extra}
        assert_error {*syntax*} {r slflush c WAIT 0 extra}
        assert_error {*syntax*} {r slflush c WAIT}
        assert_error {*timeout*} {r slflush c WAIT never}
        assert_error {*syntax*} {r slcompact c FULL extra}
    }
}

start_server {tags {slab "external:skip"}} {
    test {Overwriting one hot key freezes the memtable on its versions} {
        r del c
        r slcreate c FLUSH_ENTRIES 100
        set before [dict get [r slinfo c FULL] flushes]
        for {set i 0} {$i < 2000} {incr i} { r slmset c hot $i }
        # 2000 versions of one key reach the 4 x FLUSH_ENTRIES record trigger repeatedly.
        wait_for_condition 100 20 {[dict get [r slinfo c FULL] flushes] > $before} else {
            fail "a hot key never froze its memtable"
        }
        assert {[dict get [r slinfo c] pending_bytes] < 1000000}
        assert_equal 1999 [r slget c hot]
    }

    test {Collections are evicted like any key under allkeys-lru} {
        r flushall
        r config set maxmemory-policy allkeys-lru
        for {set j 0} {$j < 20} {incr j} {
            for {set i 0} {$i < 2000} {incr i 100} {
                set args {}
                for {set k $i} {$k < $i + 100} {incr k} { lappend args k$k [string repeat x 100] }
                r slmset c$j {*}$args
            }
        }
        set used [s used_memory]
        r config set maxmemory [expr {$used / 2}]
        # The write triggers eviction; it may still be refused if freeing (lazyfree)
        # lags behind, as it does under sanitizers.
        catch {r set trigger 1}
        wait_for_condition 100 20 {[s evicted_keys] > 0} else { fail "nothing evicted" }
        assert {[r dbsize] < 21}
        r config set maxmemory 0
        r config set maxmemory-policy noeviction
    } {OK} {needs:config-maxmemory}

    test {Deleting a large collection frees it through lazyfree} {
        r flushall
        r config set lazyfree-lazy-user-del yes
        # Small blocks: hundreds of leaves, an effort far above the lazyfree threshold.
        r slcreate big FLUSH_ENTRIES 5000 BLOCK_BYTES 1024
        for {set i 0} {$i < 50000} {incr i 500} {
            set args {}
            for {set k $i} {$k < $i + 500} {incr k} { lappend args [format k%06d $k] v }
            r slmset big {*}$args
        }
        r slflush big WAIT 0
        set before [s lazyfreed_objects]
        r del big
        wait_for_condition 100 20 {[s lazyfreed_objects] > $before} else { fail "not freed lazily" }
    }
}

start_server {tags {slab "external:skip" defrag} overrides {save {}}} {
    if {[string match {*jemalloc*} [s mem_allocator]] && [r debug mallctl arenas.page] <= 8192} {
        test {Active defrag moves fragmented slab blocks and keeps the content} {
            r config set activedefrag no
            # Many small collections with small blocks, interleaved; deleting every other
            # one leaves the survivors' blocks in half-empty allocator slabs.
            for {set j 0} {$j < 400} {incr j} {
                r slcreate c$j FLUSH_ENTRIES 400 BLOCK_BYTES 1024
                set args {}
                for {set i 0} {$i < 400} {incr i} { lappend args [format k%04d $i] [string repeat v 40] }
                r slmset c$j {*}$args
            }
            for {set j 0} {$j < 400} {incr j} { r slflush c$j WAIT 0 }
            set digest [debug_digest]
            for {set j 0} {$j < 400} {incr j 2} { r del c$j }
            r config set active-defrag-ignore-bytes 1
            r config set active-defrag-threshold-lower 1
            r config set active-defrag-cycle-min 50
            r config set active-defrag-cycle-max 75
            r config set activedefrag yes
            proc defragged {} {
                set n 0
                for {set j 1} {$j < 400} {incr j 2} { incr n [dict get [r slinfo c$j FULL] defragged_leaves] }
                return $n
            }
            wait_for_condition 200 50 {[defragged] > 0} else { fail "no slab block was moved" }
            r config set activedefrag no
            for {set j 1} {$j < 400} {incr j 2} {
                wait_for_condition 100 20 {[dict get [r slinfo c$j] compaction_inflight] == 0} else { fail "busy" }
                assert_equal OK [r debug slab verify c$j]
                assert_equal [string repeat v 40] [r slget c$j k0123]
            }
            for {set j 0} {$j < 400} {incr j 2} {
                r slcreate c$j FLUSH_ENTRIES 400 BLOCK_BYTES 1024
                set args {}
                for {set i 0} {$i < 400} {incr i} { lappend args [format k%04d $i] [string repeat v 40] }
                r slmset c$j {*}$args
                r slflush c$j WAIT 0
            }
            assert_equal $digest [debug_digest]
        } {} {needs:debug}
    }
}

set ::global_overrides $slab_saved_overrides_slab
