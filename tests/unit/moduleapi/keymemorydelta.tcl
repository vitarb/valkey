set testmodule [file normalize tests/modules/keymemorydelta.so]

proc memory_sum {keys} {
    set total 0
    foreach key $keys {
        set usage [r memory usage $key]
        if {$usage ne {}} { incr total $usage }
    }
    return $total
}

start_server {tags {modules}} {
    test {key memory delta event tracks creates, updates, and deletes} {
        assert_equal OK [r module load $testmodule]
        r select 0
        r set string value
        r append string -more
        r hset hash one 1 two 2
        r lpush list a b c
        r sadd set a b c
        r zadd zset 1 a 2 b
        r del list

        set keys {string hash set zset}
        set usage [r keymemorydelta.usage]
        assert_equal [memory_sum $keys] [lindex $usage 0]
        assert_equal 4 [lindex $usage 1]
    }

    test {key memory delta payload exposes the final sampled state} {
        r set payload-key payload-value
        set last [r keymemorydelta.last]
        assert_equal payload-key [lindex $last 0]
        assert_equal 0 [lindex $last 1]
        assert_equal 0 [lindex $last 2]
        assert_equal 0 [lindex $last 3]
        assert_equal 1 [lindex $last 4]
        assert_equal [r memory usage payload-key] [lindex $last 5]
    }

    test {key memory delta coalesces transactions, scripts, and functions} {
        set before [lindex [r keymemorydelta.usage] 2]
        r multi
        r set transaction one
        r append transaction two
        r append transaction three
        r exec
        assert_equal 1 [expr {[lindex [r keymemorydelta.usage] 2] - $before}]

        set before [lindex [r keymemorydelta.usage] 2]
        r eval {redis.call('HSET', KEYS[1], 'one', '1'); redis.call('HSET', KEYS[1], 'two', '2')} 1 script
        assert_equal 1 [expr {[lindex [r keymemorydelta.usage] 2] - $before}]

        r function load {#!lua name=keymemorydelta_test
server.register_function('delta_write', function(KEYS, ARGV)
    redis.call('SET', KEYS[1], ARGV[1])
    redis.call('APPEND', KEYS[1], ARGV[2])
    return 1
end)}
        set before [lindex [r keymemorydelta.usage] 2]
        r fcall delta_write 1 function first second
        assert_equal 1 [expr {[lindex [r keymemorydelta.usage] 2] - $before}]
        r function delete keymemorydelta_test

        assert_equal [memory_sum [r keys *]] [lindex [r keymemorydelta.usage] 0]
    }

    test {key memory delta tracks stream consumer group mutations} {
        set stream stream
        r xgroup create $stream group 0 mkstream
        set id [r xadd $stream * field value]
        r xreadgroup group group consumer count 1 streams $stream >
        assert_equal [memory_sum [r keys *]] [lindex [r keymemorydelta.usage] 0]
        assert_equal 1 [r xack $stream group $id]
        assert_equal [memory_sum [r keys *]] [lindex [r keymemorydelta.usage] 0]
    }

    test {key memory delta tracks aggregate rehash completed by reads} {
        for {set i 0} {$i < 2000} {incr i} {
            r hset rehash-hash field-$i [string repeat x [expr {1 + ($i % 97)}]]
            r sadd rehash-set member-$i
            r zadd rehash-zset $i member-$i
        }
        for {set i 0} {$i < 4000} {incr i} {
            set member [expr {$i % 2000}]
            r hget rehash-hash field-$member
            r sismember rehash-set member-$member
            r zscore rehash-zset member-$member
        }
        assert_equal [memory_sum [r keys *]] [lindex [r keymemorydelta.usage] 0]
    }

    test {key memory delta reports source and destination of MOVE} {
        set before0 [r keymemorydelta.usage 0]
        set before2 [r keymemorydelta.usage 2]
        r set moved value
        set db0 [r memory usage moved]
        assert_equal 1 [r move moved 2]
        assert_equal [lindex $before0 0] [lindex [r keymemorydelta.usage 0] 0]
        assert_equal [lindex $before0 1] [lindex [r keymemorydelta.usage 0] 1]
        assert_equal [expr {[lindex $before2 0] + $db0}] [lindex [r keymemorydelta.usage 2] 0]
        assert_equal [expr {[lindex $before2 1] + 1}] [lindex [r keymemorydelta.usage 2] 1]
    }

    test {key memory delta fires while an RDB is loaded} {
        r select 0
        r save
        assert_equal OK [r keymemorydelta.reset]
        r debug reload nosave
        assert_equal [memory_sum [r keys *]] [lindex [r keymemorydelta.usage] 0]
        assert_equal [r dbsize] [lindex [r keymemorydelta.usage] 1]
    }
}
