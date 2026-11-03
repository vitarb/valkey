# Slab collections in cluster mode: one collection is one key, in one slot.

proc slab_cluster_fill {idx coll n tag} {
    for {set i 0} {$i < $n} {incr i 100} {
        set args {}
        for {set j $i} {$j < $i + 100 && $j < $n} {incr j} { lappend args [format k%06d $j] "v$j:$tag" }
        R $idx slmset $coll {*}$args
    }
}

proc slab_node_for_key {key} {
    set slot [R 0 cluster keyslot $key]
    for {set i 0} {$i < 3} {incr i} {
        foreach range [dict get [get_myself $i] slots] {
            lassign [split $range -] start end
            if {$end eq {}} {set end $start}
            if {$slot >= $start && $slot <= $end} { return $i }
        }
    }
    return -1
}

start_cluster 3 3 {tags {slab external:skip cluster} overrides {cluster-allow-replica-migration no}} {
    set key {slab:{feature-a}:store}

    test {Slab commands route by the collection key and replicas serve reads} {
        set owner [slab_node_for_key $key]
        set other [expr {($owner + 1) % 3}]
        assert_error {*MOVED*} {R $other slmset $key a 1}
        R $owner slcreate $key FLUSH_ENTRIES 200 BLOCK_BYTES 2048
        slab_cluster_fill $owner $key 1000 a
        R $owner slflush $key WAIT 0
        slab_cluster_fill $owner $key 50 b
        R $owner sldel $key k000003
        set primary [srv -$owner client]
        set replica [srv -[expr {$owner + 3}] client]
        wait_for_ofs_sync $primary $replica
        $replica readonly
        assert_equal v1:b [$replica slget $key k000001]
        assert_equal {} [$replica slget $key k000003]
        assert_equal [R $owner debug digest-value $key] [$replica debug digest-value $key]
    } {} {needs:debug}

    test {Atomic slot migration moves a collection with every layer} {
        set owner [slab_node_for_key $key]
        set target [expr {($owner + 1) % 3}]
        set digest [R $owner debug digest-value $key]
        set slot [R $owner cluster keyslot $key]
        set target_id [R $target cluster myid]
        assert_match OK [R $owner cluster migrateslots slotsrange $slot $slot node $target_id]
        wait_for_condition 100 100 {[slab_node_for_key $key] == $target} else {
            fail "slot $slot did not move: [R $owner cluster getslotmigrations]"
        }
        wait_for_cluster_propagation
        assert_equal $digest [R $target debug digest-value $key]
        assert_equal OK [R $target debug slab verify $key]
        assert_equal 0 [R $owner cluster countkeysinslot $slot]
        # The migrated collection keeps its options and accepts writes.
        assert_equal 2048 [dict get [R $target slinfo $key FULL] block_bytes]
        R $target slmset $key k000001 after-migration
        assert_equal after-migration [R $target slget $key k000001]
    } {} {needs:debug}

    test {MIGRATE moves a collection between nodes} {
        set src [slab_node_for_key $key]
        set dst [expr {($src + 1) % 3}]
        set digest [R $src debug digest-value $key]
        set slot [R $src cluster keyslot $key]
        set dst_id [R $dst cluster myid]
        set src_id [R $src cluster myid]
        R $dst cluster setslot $slot importing $src_id
        R $src cluster setslot $slot migrating $dst_id
        R $src migrate [srv -$dst host] [srv -$dst port] $key 0 5000
        R $dst cluster setslot $slot node $dst_id
        R $src cluster setslot $slot node $dst_id
        wait_for_cluster_propagation
        assert_equal $digest [R $dst debug digest-value $key]
        assert_equal 0 [R $src cluster countkeysinslot $slot]
    } {} {needs:debug}
}

start_cluster 1 0 {tags {slab external:skip cluster} overrides {io-threads 3 io-threads-always-active yes cluster-slot-stats-enabled yes}} {
    test {A reply built off the main thread counts toward its slot's network-bytes-out} {
        set key {slab:{stats}}
        slab_cluster_fill 0 $key 100 x
        set slot [R 0 cluster keyslot $key]
        set keys {}
        for {set j 0} {$j < 100} {incr j} { lappend keys [format k%06d $j] }
        R 0 config resetstat
        assert_equal v99:x [lindex [R 0 slmget $key {*}$keys] 99]
        assert {[getInfoProperty [R 0 info debug] slab_reads_dispatched] >= 1}
        set want [string length "*100\r\n"]
        for {set j 0} {$j < 100} {incr j} {
            incr want [string length "\$[string length v$j:x]\r\nv$j:x\r\n"]
        }
        set stats [R 0 cluster slot-stats slotsrange $slot $slot]
        assert_equal $want [dict get [lindex $stats 0 1] network-bytes-out]
    }
}
