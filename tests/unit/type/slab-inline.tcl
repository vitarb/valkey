# The slab suites again with no maintenance threads: maintenance runs on the main
# thread at notify. Results must not depend on the thread counts; tests
# that hold maintenance threads are tagged slab:threads and skipped here.

set slab_saved_overrides $::global_overrides
set slab_saved_denytags $::denytags
lappend ::global_overrides slab-maintenance-threads 0
lappend ::denytags slab:threads

source tests/unit/type/slab.tcl

set ::global_overrides $slab_saved_overrides
set ::denytags $slab_saved_denytags
