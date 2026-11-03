# Differential fuzzing of slab collections against a Tcl dict model: random writes and
# reads, with flushes, compactions, maintenance pauses, reloads from RDB and AOF, and a
# replica compared by digest, all at random points. SLAB_FUZZ_ITERS raises the length.

proc slab_fuzz_key {} {
    # Keys share long prefixes and collide often, like a feature store's.
    return [format "src:%d:entity_%06d" [randomInt 3] [randomInt 400]]
}

proc slab_fuzz_value {} {
    set n [randomInt 4]
    if {$n == 0} { return "" }
    if {$n == 3} { return [string repeat [randomInt 10] [expr {1 + [randomInt 300]}]] }
    return "v[randomInt 100000]"
}

# The model's value for a key, or "" when absent. Not expr: it would turn "000" into 0.
proc slab_fuzz_want {model k} {
    if {[dict exists $model $k]} { return [dict get $model $k] }
    return ""
}

proc slab_fuzz_check {model coll {level -1}} {
    set keys [dict keys $model]
    for {set i 0} {$i < 400} {incr i} { lappend keys [format "src:%d:entity_%06d" [randomInt 3] [randomInt 400]] }
    set got [r $level slmget $coll {*}$keys]
    set i 0
    foreach k $keys {
        set want [slab_fuzz_want $model $k]
        assert_equal $want [lindex $got $i] "key $k"
        incr i
    }
    # A full scan in key order equals the sorted model.
    set scanned {}
    set cursor 0
    while 1 {
        set res [r $level slscan $coll $cursor COUNT [expr {1 + [randomInt 200]}]]
        lappend scanned {*}[lindex $res 1]
        set cursor [lindex $res 0]
        if {$cursor eq "0"} break
    }
    set expected {}
    foreach k [lsort [dict keys $model]] { lappend expected $k [dict get $model $k] }
    assert_equal $expected $scanned
    assert_equal [dict size $model] [r $level slcount $coll EXACT]
    assert_equal OK [r $level debug slab verify $coll]
}

# A write refused with -SLABBUSY (maintenance paused) changes nothing: returns 0.
proc slab_fuzz_write {args} {
    if {[catch {r -1 {*}$args} err]} {
        if {[string match {*SLABBUSY*} $err]} { return 0 }
        error $err
    }
    return 1
}

start_server {tags {slab slow "external:skip"} overrides {appendonly yes io-threads 3 io-threads-always-active yes}} {
    start_server {} {
        set replica [srv 0 client]
        set primary [srv -1 client]
        $replica replicaof [srv -1 host] [srv -1 port]
        wait_for_sync $replica

        test {Slab collections match a model under random operations} {
            set iters [expr {[info exists ::env(SLAB_FUZZ_ITERS)] ? $::env(SLAB_FUZZ_ITERS) : 3000}]
            set model [dict create]
            r -1 slcreate f FLUSH_ENTRIES [expr {20 + [randomInt 300]}] BLOCK_BYTES [expr {1024 + [randomInt 4096]}]
            for {set it 0} {$it < $iters} {incr it} {
                set op [randomInt 100]
                if {$op < 50} {
                    set args {}
                    for {set j [randomInt 60]} {$j >= 0} {incr j -1} {
                        lappend args [slab_fuzz_key] [slab_fuzz_value]
                    }
                    if {[slab_fuzz_write slmset f {*}$args]} {
                        foreach {k v} $args { dict set model $k $v }
                    }
                } elseif {$op < 70} {
                    set args {}
                    for {set j [randomInt 20]} {$j >= 0} {incr j -1} { lappend args [slab_fuzz_key] }
                    if {[slab_fuzz_write sldel f {*}$args]} {
                        foreach k $args { dict unset model $k }
                    }
                } elseif {$op < 80} {
                    set k [slab_fuzz_key]
                    set want [slab_fuzz_want $model $k]
                    assert_equal $want [r -1 slget f $k]
                } elseif {$op < 84} {
                    r -1 slflush f
                } elseif {$op < 87} {
                    r -1 slcompact f
                } elseif {$op < 89} {
                    r -1 slcompact f FULL
                } elseif {$op < 91} {
                    r -1 debug slab pause-maintenance
                } elseif {$op < 93} {
                    r -1 debug slab resume-maintenance
                } elseif {$op < 95 && $it % 7 == 0} {
                    r -1 debug slab resume-maintenance
                    waitForBgrewriteaof $primary
                    r -1 debug reload
                } elseif {$op < 96 && $it % 11 == 0} {
                    r -1 debug slab resume-maintenance
                    waitForBgrewriteaof $primary
                    r -1 debug loadaof
                } elseif {$op < 97 && $it % 13 == 0} {
                    waitForBgrewriteaof $primary
                    r -1 bgrewriteaof
                } else {
                    set primary_digest [r -1 debug digest-value f]
                    wait_for_ofs_sync $primary $replica
                    assert_equal $primary_digest [$replica debug digest-value f]
                }
                if {$it % 500 == 499} {
                    r -1 debug slab resume-maintenance
                    slab_fuzz_check $model f
                }
            }
            r -1 debug slab resume-maintenance
            r -1 slflush f WAIT 0
            slab_fuzz_check $model f
            waitForBgrewriteaof $primary
            r -1 debug loadaof
            slab_fuzz_check $model f
            wait_for_ofs_sync $primary $replica
            assert_equal [r -1 debug digest-value f] [$replica debug digest-value f]
        } {} {needs:debug}
    }
}
