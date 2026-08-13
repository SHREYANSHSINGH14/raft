package types

import (
	"testing"

	"github.com/SHREYANSHSINGH14/raft"
	"github.com/stretchr/testify/assert"
)

// The whole point of this test is that it fails when a field is added to
// raft.LogEntry and not to the converter — which is exactly how Type went
// missing through three separate hand-written converters.
func TestLogEntry_RoundTrip_PreservesEveryField(t *testing.T) {
	for _, typ := range []raft.EntryType{
		raft.EntryType_Command,
		raft.EntryType_NoOp,
		raft.EntryType_Config,
		raft.EntryType_Barrier,
	} {
		in := raft.LogEntry{Index: 7, Term: 3, Type: typ, Data: []byte("payload")}
		assert.Equal(t, in, LogEntryToRaft(LogEntryFromRaft(in)), "round trip lost a field for type %d", typ)
	}
}

// A cast instead of the explicit switch would make this pass for Command and
// fail for everything after it, since the enums are offset by one.
func TestEntryType_MapsAcrossTheOffset(t *testing.T) {
	assert.Equal(t, EntryType_ENTRY_TYPE_COMMAND, entryTypeFromRaft(raft.EntryType_Command))
	assert.Equal(t, EntryType_ENTRY_TYPE_NO_OP, entryTypeFromRaft(raft.EntryType_NoOp))
	assert.Equal(t, EntryType_ENTRY_TYPE_CONFIG, entryTypeFromRaft(raft.EntryType_Config))
	assert.Equal(t, EntryType_ENTRY_TYPE_BARRIER, entryTypeFromRaft(raft.EntryType_Barrier))
}

// Entries written before Type was carried have no Type field, which decodes to
// UNSPECIFIED. They must keep behaving as commands rather than becoming an
// entry type the library would act on.
func TestEntryType_UnspecifiedDecodesAsCommand(t *testing.T) {
	assert.Equal(t, raft.EntryType_Command, entryTypeToRaft(EntryType_ENTRY_TYPE_UNSPECIFIED))
}

func TestLogEntryToRaft_NilIsZeroValue(t *testing.T) {
	assert.Equal(t, raft.LogEntry{}, LogEntryToRaft(nil))
}
