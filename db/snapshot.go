package db

type snapshot struct {
	prev           *snapshot
	next           *snapshot
	sequenceNumber sequenceNumber
}

func newSnapshot(seq sequenceNumber) *snapshot {
	return &snapshot{sequenceNumber: seq}
}

type snapshotList struct {
	head snapshot
}

func newSnapshotList() *snapshotList {
	head := newSnapshot(0)
	head.prev = head
	head.next = head
	return &snapshotList{head: *head}
}

func (l *snapshotList) empty() bool {
	return l.head.next == &l.head
}

func (l *snapshotList) oldest() *snapshot {
	if l.empty() {
		panic("snapshotList: empty()")
	}
	return l.head.next
}

func (l *snapshotList) newest() *snapshot {
	if l.empty() {
		panic("snapshotList: empty()")
	}
	return l.head.prev
}

func (l *snapshotList) newSnapshot(seq sequenceNumber) *snapshot {
	if !l.empty() && l.newest().sequenceNumber > seq {
		panic("snapshotList: !empty() && newest().sequenceNumber > seq")
	}
	snapshot := newSnapshot(seq)
	snapshot.next = &l.head
	snapshot.prev = l.head.prev
	snapshot.prev.next = snapshot
	snapshot.next.prev = snapshot
	return snapshot
}

func (l *snapshotList) deleteSnapshot(s *snapshot) {
	s.prev.next = s.next
	s.next.prev = s.prev
}
