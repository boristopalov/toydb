## Example showing how how indexes are used (thx Claude)

### Initial state

Leader:

- log: [1, 2, 3, 4, 5] (entries with index 1-5)
- commitIndex: 3 (entries 1-3 are committed)
- nextIndex: {
  "Follower1": 6, (will send entry 6 next, assumes Follower1 has 1-5)
  "Follower2": 4 (will send entry 4 next, assumes Follower2 has 1-3)
  }
- matchIndex: {
  "Follower1": 5, (knows Follower1 has entries 1-5)
  "Follower2": 3 (knows Follower2 has entries 1-3)
  }

Follower1:

- log: [1, 2, 3, 4, 5] (fully up-to-date)
- commitIndex: 3 (entries 1-3 are committed)

Follower2:

- log: [1, 2, 3] (missing entries 4-5)
- commitIndex: 3 (entries 1-3 are committed)

### Step 1 - Client sends command

Leader:

- log: [1, 2, 3, 4, 5, 6] (added new entry 6)
- commitIndex: 3 (still 3, not committed yet)
- nextIndex: unchanged
- matchIndex: unchanged

### Step 2 - Leader replicates to followers

The leader sends AppendEntries RPCs to each follower:
To Follower1: Sends entry 6 (because nextIndex["Follower1"] = 6)
To Follower2: Sends entries 4, 5, 6 (because nextIndex["Follower2"] = 4)

### Step 3 - Follower1 accepts AppendEntries from leader

Leader:

- nextIndex["Follower1"]: 7 (incremented)
- matchIndex["Follower1"]: 6 (updated to reflect successful replication)

Follower1:

- log: [1, 2, 3, 4, 5, 6] (added entry 6)
- commitIndex: 3 (still 3, waiting for leader to commit)

### Step 4 - Follower2 rejects AppendENtries from leader due to a log inconsistency between the leader and follower2

Leader:

- nextIndex["Follower2"]: 3 (decremented to try again)
- matchIndex["Follower2"]: 3 (unchanged)

### Step 5 - Leader retries AppendEntries to Follower2

Leader:

- nextIndex["Follower2"]: 7 (incremented to 7)
- matchIndex["Follower2"]: 6 (updated to 6)

Follower2:

- log: [1, 2, 3, 4, 5, 6] (now has all entries)
- commitIndex: 3 (still 3, waiting for leader to commit)

### Step 6 - Leader updates commitIndex

Leader:

- commitIndex: 6 (updated because a majority have replicated entries 4-6)

### Step 7 - Leader sends new commitIndex to followers

Follower1 and Follower2:

- commitIndex: 6 (updated based on leader's message)

### Decrementing nextIndex

The decrementing of nextIndex implements Raft's log consistency check mechanism. By decrementing nextIndex, the leader is essentially saying "Let's try an earlier point in the log where we might agree." Worst case scenario of this is O(n) where n == number of entries in the log up to the last index in the follower. This results in the entire log being sent to the follower.

In practice there are better ways to do this, e.g. binary search on the log based on nextIndex value of the follower

### matchIndex purpose

For each commitIndex N (of the leader), the leader checks if a majority of matchIndex values are ≥ N
If true, and the entry at index N is from the leader's current term, then N can be committed

When a follower successfully replicates entries: nextIndex[follower] = matchIndex[follower] + 1

After a successful commit:

- The leader's commitIndex will be updated to the highest index that is safely replicated on a majority of servers. commitIndex is basically a cluster-wide safety threshold
- For each follower, one of these conditions will be true:
  matchIndex[follower] > commitIndex (this follower is ahead of the commit point)
  matchIndex[follower] = commitIndex (this follower is exactly at the commit point)
  matchIndex[follower] < commitIndex (this follower is lagging behind)
- By definition of commitment in Raft:
  A majority of nodes will have matchIndex ≥ commitIndex, since an entry is considered committed only when a majority of nodes have replicated/accepted that entry
  Some followers might still be catching up
