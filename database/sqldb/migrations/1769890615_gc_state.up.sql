-- Add GC state tracking to groups table
-- gc_state values:
--   0: active (default) - group is live and should have claims extended
--   1: gc_candidate - group has no live references, candidate for GC
--   2: gc_confirmed - confirmed for GC, claims will not be extended
--   3: gc_complete - claims have expired, group can be cleaned up

ALTER TABLE groups ADD COLUMN IF NOT EXISTS gc_state INTEGER DEFAULT 0;

-- Track block reference counts for GC decisions
-- live_blocks: blocks with active S3 object references
-- dead_blocks: blocks with no references (orphaned)
-- dead_bytes: total size of dead blocks for space accounting
ALTER TABLE groups ADD COLUMN IF NOT EXISTS live_blocks BIGINT DEFAULT 0;
ALTER TABLE groups ADD COLUMN IF NOT EXISTS dead_blocks BIGINT DEFAULT 0;
ALTER TABLE groups ADD COLUMN IF NOT EXISTS dead_bytes BIGINT DEFAULT 0;

-- Track when group was marked for GC
ALTER TABLE groups ADD COLUMN IF NOT EXISTS gc_marked_at TIMESTAMP;

-- Index for efficient GC queries
CREATE INDEX IF NOT EXISTS groups_gc_state_index ON groups (gc_state);
CREATE INDEX IF NOT EXISTS groups_gc_marked_at_index ON groups (gc_marked_at) WHERE gc_marked_at IS NOT NULL;
