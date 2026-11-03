# The slab suites again with no threads: maintenance runs at notify and every read
# inline on the main thread. Results must not depend on the thread counts; tests that
# hold maintenance threads or reads are tagged slab:threads and skipped here.

set slab_saved_overrides $::global_overrides
set slab_saved_denytags $::denytags
lappend ::global_overrides slab-maintenance-threads 0 io-threads 1
lappend ::denytags slab:threads

source tests/unit/type/slab.tcl
source tests/unit/type/slab-persistence.tcl

set ::global_overrides $slab_saved_overrides
set ::denytags $slab_saved_denytags
