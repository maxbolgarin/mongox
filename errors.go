package mongox

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Common errors
var (
	ErrNotFound        = errors.New("not found")
	ErrDuplicate       = errors.New("duplicate")
	ErrInvalidArgument = errors.New("invalid client argument")
	ErrInternal        = errors.New("internal error")
	ErrNetwork         = errors.New("network error")
	ErrTimeout         = errors.New("timeout")
	ErrBadServer       = errors.New("bad server")
)

// Mongo errors from codes
var (
	InternalError                                               = errors.New("InternalError, code 1")
	BadValue                                                    = errors.New("BadValue, code 2")
	NoSuchKey                                                   = errors.New("NoSuchKey, code 4")
	GraphContainsCycle                                          = errors.New("GraphContainsCycle, code 5")
	HostUnreachable                                             = errors.New("HostUnreachable, code 6")
	HostNotFound                                                = errors.New("HostNotFound, code 7")
	UnknownError                                                = errors.New("UnknownError, code 8")
	FailedToParse                                               = errors.New("FailedToParse, code 9")
	CannotMutateObject                                          = errors.New("CannotMutateObject, code 10")
	UserNotFound                                                = errors.New("UserNotFound, code 11")
	UnsupportedFormat                                           = errors.New("UnsupportedFormat, code 12")
	Unauthorized                                                = errors.New("Unauthorized, code 13")
	TypeMismatch                                                = errors.New("TypeMismatch, code 14")
	Overflow                                                    = errors.New("Overflow, code 15")
	InvalidLength                                               = errors.New("InvalidLength, code 16")
	ProtocolError                                               = errors.New("ProtocolError, code 17")
	AuthenticationFailed                                        = errors.New("AuthenticationFailed, code 18")
	CannotReuseObject                                           = errors.New("CannotReuseObject, code 19")
	IllegalOperation                                            = errors.New("IllegalOperation, code 20")
	EmptyArrayOperation                                         = errors.New("EmptyArrayOperation, code 21")
	InvalidBSON                                                 = errors.New("InvalidBSON, code 22")
	AlreadyInitialized                                          = errors.New("AlreadyInitialized, code 23")
	LockTimeout                                                 = errors.New("LockTimeout, code 24")
	RemoteValidationError                                       = errors.New("RemoteValidationError, code 25")
	NamespaceNotFound                                           = errors.New("NamespaceNotFound, code 26")
	IndexNotFound                                               = errors.New("IndexNotFound, code 27")
	PathNotViable                                               = errors.New("PathNotViable, code 28")
	NonExistentPath                                             = errors.New("NonExistentPath, code 29")
	InvalidPath                                                 = errors.New("InvalidPath, code 30")
	RoleNotFound                                                = errors.New("RoleNotFound, code 31")
	RolesNotRelated                                             = errors.New("RolesNotRelated, code 32")
	PrivilegeNotFound                                           = errors.New("PrivilegeNotFound, code 33")
	CannotBackfillArray                                         = errors.New("CannotBackfillArray, code 34")
	UserModificationFailed                                      = errors.New("UserModificationFailed, code 35")
	RemoteChangeDetected                                        = errors.New("RemoteChangeDetected, code 36")
	FileRenameFailed                                            = errors.New("FileRenameFailed, code 37")
	FileNotOpen                                                 = errors.New("FileNotOpen, code 38")
	FileStreamFailed                                            = errors.New("FileStreamFailed, code 39")
	ConflictingUpdateOperators                                  = errors.New("ConflictingUpdateOperators, code 40")
	FileAlreadyOpen                                             = errors.New("FileAlreadyOpen, code 41")
	LogWriteFailed                                              = errors.New("LogWriteFailed, code 42")
	CursorNotFound                                              = errors.New("CursorNotFound, code 43")
	UserDataInconsistent                                        = errors.New("UserDataInconsistent, code 45")
	LockBusy                                                    = errors.New("LockBusy, code 46")
	NoMatchingDocument                                          = errors.New("NoMatchingDocument, code 47")
	NamespaceExists                                             = errors.New("NamespaceExists, code 48")
	InvalidRoleModification                                     = errors.New("InvalidRoleModification, code 49")
	MaxTimeMSExpired                                            = errors.New("MaxTimeMSExpired, code 50")
	ManualInterventionRequired                                  = errors.New("ManualInterventionRequired, code 51")
	DollarPrefixedFieldName                                     = errors.New("DollarPrefixedFieldName, code 52")
	InvalidIdField                                              = errors.New("InvalidIdField, code 53")
	NotSingleValueField                                         = errors.New("NotSingleValueField, code 54")
	InvalidDBRef                                                = errors.New("InvalidDBRef, code 55")
	EmptyFieldName                                              = errors.New("EmptyFieldName, code 56")
	DottedFieldName                                             = errors.New("DottedFieldName, code 57")
	RoleModificationFailed                                      = errors.New("RoleModificationFailed, code 58")
	CommandNotFound                                             = errors.New("CommandNotFound, code 59")
	ShardKeyNotFound                                            = errors.New("ShardKeyNotFound, code 61")
	OplogOperationUnsupported                                   = errors.New("OplogOperationUnsupported, code 62")
	StaleShardVersion                                           = errors.New("StaleShardVersion, code 63")
	WriteConcernFailed                                          = errors.New("WriteConcernFailed, code 64")
	MultipleErrorsOccurred                                      = errors.New("MultipleErrorsOccurred, code 65")
	ImmutableField                                              = errors.New("ImmutableField, code 66")
	CannotCreateIndex                                           = errors.New("CannotCreateIndex, code 67")
	IndexAlreadyExists                                          = errors.New("IndexAlreadyExists, code 68")
	AuthSchemaIncompatible                                      = errors.New("AuthSchemaIncompatible, code 69")
	ShardNotFound                                               = errors.New("ShardNotFound, code 70")
	ReplicaSetNotFound                                          = errors.New("ReplicaSetNotFound, code 71")
	InvalidOptions                                              = errors.New("InvalidOptions, code 72")
	InvalidNamespace                                            = errors.New("InvalidNamespace, code 73")
	NodeNotFound                                                = errors.New("NodeNotFound, code 74")
	WriteConcernLegacyOK                                        = errors.New("WriteConcernLegacyOK, code 75")
	NoReplicationEnabled                                        = errors.New("NoReplicationEnabled, code 76")
	OperationIncomplete                                         = errors.New("OperationIncomplete, code 77")
	CommandResultSchemaViolation                                = errors.New("CommandResultSchemaViolation, code 78")
	UnknownReplWriteConcern                                     = errors.New("UnknownReplWriteConcern, code 79")
	RoleDataInconsistent                                        = errors.New("RoleDataInconsistent, code 80")
	NoMatchParseContext                                         = errors.New("NoMatchParseContext, code 81")
	NoProgressMade                                              = errors.New("NoProgressMade, code 82")
	RemoteResultsUnavailable                                    = errors.New("RemoteResultsUnavailable, code 83")
	IndexOptionsConflict                                        = errors.New("IndexOptionsConflict, code 85")
	IndexKeySpecsConflict                                       = errors.New("IndexKeySpecsConflict, code 86")
	CannotSplit                                                 = errors.New("CannotSplit, code 87")
	NetworkTimeout                                              = errors.New("NetworkTimeout, code 89")
	CallbackCanceled                                            = errors.New("CallbackCanceled, code 90")
	ShutdownInProgress                                          = errors.New("ShutdownInProgress, code 91")
	SecondaryAheadOfPrimary                                     = errors.New("SecondaryAheadOfPrimary, code 92")
	InvalidReplicaSetConfig                                     = errors.New("InvalidReplicaSetConfig, code 93")
	NotYetInitialized                                           = errors.New("NotYetInitialized, code 94")
	NotSecondary                                                = errors.New("NotSecondary, code 95")
	OperationFailed                                             = errors.New("OperationFailed, code 96")
	NoProjectionFound                                           = errors.New("NoProjectionFound, code 97")
	DBPathInUse                                                 = errors.New("DBPathInUse, code 98")
	UnsatisfiableWriteConcern                                   = errors.New("UnsatisfiableWriteConcern, code 100")
	OutdatedClient                                              = errors.New("OutdatedClient, code 101")
	IncompatibleAuditMetadata                                   = errors.New("IncompatibleAuditMetadata, code 102")
	NewReplicaSetConfigurationIncompatible                      = errors.New("NewReplicaSetConfigurationIncompatible, code 103")
	NodeNotElectable                                            = errors.New("NodeNotElectable, code 104")
	IncompatibleShardingMetadata                                = errors.New("IncompatibleShardingMetadata, code 105")
	DistributedClockSkewed                                      = errors.New("DistributedClockSkewed, code 106")
	LockFailed                                                  = errors.New("LockFailed, code 107")
	InconsistentReplicaSetNames                                 = errors.New("InconsistentReplicaSetNames, code 108")
	ConfigurationInProgress                                     = errors.New("ConfigurationInProgress, code 109")
	CannotInitializeNodeWithData                                = errors.New("CannotInitializeNodeWithData, code 110")
	NotExactValueField                                          = errors.New("NotExactValueField, code 111")
	WriteConflict                                               = errors.New("WriteConflict, code 112")
	InitialSyncFailure                                          = errors.New("InitialSyncFailure, code 113")
	InitialSyncOplogSourceMissing                               = errors.New("InitialSyncOplogSourceMissing, code 114")
	CommandNotSupported                                         = errors.New("CommandNotSupported, code 115")
	DocTooLargeForCapped                                        = errors.New("DocTooLargeForCapped, code 116")
	ConflictingOperationInProgress                              = errors.New("ConflictingOperationInProgress, code 117")
	NamespaceNotSharded                                         = errors.New("NamespaceNotSharded, code 118")
	InvalidSyncSource                                           = errors.New("InvalidSyncSource, code 119")
	OplogStartMissing                                           = errors.New("OplogStartMissing, code 120")
	DocumentValidationFailure                                   = errors.New("DocumentValidationFailure, code 121")
	NotAReplicaSet                                              = errors.New("NotAReplicaSet, code 123")
	IncompatibleElectionProtocol                                = errors.New("IncompatibleElectionProtocol, code 124")
	CommandFailed                                               = errors.New("CommandFailed, code 125")
	RPCProtocolNegotiationFailed                                = errors.New("RPCProtocolNegotiationFailed, code 126")
	UnrecoverableRollbackError                                  = errors.New("UnrecoverableRollbackError, code 127")
	LockNotFound                                                = errors.New("LockNotFound, code 128")
	LockStateChangeFailed                                       = errors.New("LockStateChangeFailed, code 129")
	SymbolNotFound                                              = errors.New("SymbolNotFound, code 130")
	FailedToSatisfyReadPreference                               = errors.New("FailedToSatisfyReadPreference, code 133")
	ReadConcernMajorityNotAvailableYet                          = errors.New("ReadConcernMajorityNotAvailableYet, code 134")
	StaleTerm                                                   = errors.New("StaleTerm, code 135")
	CappedPositionLost                                          = errors.New("CappedPositionLost, code 136")
	IncompatibleShardingConfigVersion                           = errors.New("IncompatibleShardingConfigVersion, code 137")
	RemoteOplogStale                                            = errors.New("RemoteOplogStale, code 138")
	JSInterpreterFailure                                        = errors.New("JSInterpreterFailure, code 139")
	InvalidSSLConfiguration                                     = errors.New("InvalidSSLConfiguration, code 140")
	SSLHandshakeFailed                                          = errors.New("SSLHandshakeFailed, code 141")
	JSUncatchableError                                          = errors.New("JSUncatchableError, code 142")
	CursorInUse                                                 = errors.New("CursorInUse, code 143")
	IncompatibleCatalogManager                                  = errors.New("IncompatibleCatalogManager, code 144")
	PooledConnectionsDropped                                    = errors.New("PooledConnectionsDropped, code 145")
	ExceededMemoryLimit                                         = errors.New("ExceededMemoryLimit, code 146")
	ZLibError                                                   = errors.New("ZLibError, code 147")
	ReadConcernMajorityNotEnabled                               = errors.New("ReadConcernMajorityNotEnabled, code 148")
	NoConfigPrimary                                             = errors.New("NoConfigPrimary, code 149")
	StaleEpoch                                                  = errors.New("StaleEpoch, code 150")
	OperationCannotBeBatched                                    = errors.New("OperationCannotBeBatched, code 151")
	OplogOutOfOrder                                             = errors.New("OplogOutOfOrder, code 152")
	ChunkTooBig                                                 = errors.New("ChunkTooBig, code 153")
	InconsistentShardIdentity                                   = errors.New("InconsistentShardIdentity, code 154")
	CannotApplyOplogWhilePrimary                                = errors.New("CannotApplyOplogWhilePrimary, code 155")
	CanRepairToDowngrade                                        = errors.New("CanRepairToDowngrade, code 157")
	MustUpgrade                                                 = errors.New("MustUpgrade, code 158")
	DurationOverflow                                            = errors.New("DurationOverflow, code 159")
	MaxStalenessOutOfRange                                      = errors.New("MaxStalenessOutOfRange, code 160")
	IncompatibleCollationVersion                                = errors.New("IncompatibleCollationVersion, code 161")
	CollectionIsEmpty                                           = errors.New("CollectionIsEmpty, code 162")
	ZoneStillInUse                                              = errors.New("ZoneStillInUse, code 163")
	InitialSyncActive                                           = errors.New("InitialSyncActive, code 164")
	ViewDepthLimitExceeded                                      = errors.New("ViewDepthLimitExceeded, code 165")
	CommandNotSupportedOnView                                   = errors.New("CommandNotSupportedOnView, code 166")
	OptionNotSupportedOnView                                    = errors.New("OptionNotSupportedOnView, code 167")
	InvalidPipelineOperator                                     = errors.New("InvalidPipelineOperator, code 168")
	CommandOnShardedViewNotSupportedOnMongod                    = errors.New("CommandOnShardedViewNotSupportedOnMongod, code 169")
	TooManyMatchingDocuments                                    = errors.New("TooManyMatchingDocuments, code 170")
	CannotIndexParallelArrays                                   = errors.New("CannotIndexParallelArrays, code 171")
	TransportSessionClosed                                      = errors.New("TransportSessionClosed, code 172")
	TransportSessionNotFound                                    = errors.New("TransportSessionNotFound, code 173")
	TransportSessionUnknown                                     = errors.New("TransportSessionUnknown, code 174")
	QueryPlanKilled                                             = errors.New("QueryPlanKilled, code 175")
	FileOpenFailed                                              = errors.New("FileOpenFailed, code 176")
	ZoneNotFound                                                = errors.New("ZoneNotFound, code 177")
	RangeOverlapConflict                                        = errors.New("RangeOverlapConflict, code 178")
	WindowsPdhError                                             = errors.New("WindowsPdhError, code 179")
	BadPerfCounterPath                                          = errors.New("BadPerfCounterPath, code 180")
	AmbiguousIndexKeyPattern                                    = errors.New("AmbiguousIndexKeyPattern, code 181")
	InvalidViewDefinition                                       = errors.New("InvalidViewDefinition, code 182")
	ClientMetadataMissingField                                  = errors.New("ClientMetadataMissingField, code 183")
	ClientMetadataAppNameTooLarge                               = errors.New("ClientMetadataAppNameTooLarge, code 184")
	ClientMetadataDocumentTooLarge                              = errors.New("ClientMetadataDocumentTooLarge, code 185")
	ClientMetadataCannotBeMutated                               = errors.New("ClientMetadataCannotBeMutated, code 186")
	LinearizableReadConcernError                                = errors.New("LinearizableReadConcernError, code 187")
	IncompatibleServerVersion                                   = errors.New("IncompatibleServerVersion, code 188")
	PrimarySteppedDown                                          = errors.New("PrimarySteppedDown, code 189")
	MasterSlaveConnectionFailure                                = errors.New("MasterSlaveConnectionFailure, code 190")
	FailPointEnabled                                            = errors.New("FailPointEnabled, code 192")
	NoShardingEnabled                                           = errors.New("NoShardingEnabled, code 193")
	BalancerInterrupted                                         = errors.New("BalancerInterrupted, code 194")
	ViewPipelineMaxSizeExceeded                                 = errors.New("ViewPipelineMaxSizeExceeded, code 195")
	InvalidIndexSpecificationOption                             = errors.New("InvalidIndexSpecificationOption, code 197")
	ReplicaSetMonitorRemoved                                    = errors.New("ReplicaSetMonitorRemoved, code 199")
	ChunkRangeCleanupPending                                    = errors.New("ChunkRangeCleanupPending, code 200")
	CannotBuildIndexKeys                                        = errors.New("CannotBuildIndexKeys, code 201")
	NetworkInterfaceExceededTimeLimit                           = errors.New("NetworkInterfaceExceededTimeLimit, code 202")
	ShardingStateNotInitialized                                 = errors.New("ShardingStateNotInitialized, code 203")
	TimeProofMismatch                                           = errors.New("TimeProofMismatch, code 204")
	ClusterTimeFailsRateLimiter                                 = errors.New("ClusterTimeFailsRateLimiter, code 205")
	NoSuchSession                                               = errors.New("NoSuchSession, code 206")
	InvalidUUID                                                 = errors.New("InvalidUUID, code 207")
	TooManyLocks                                                = errors.New("TooManyLocks, code 208")
	StaleClusterTime                                            = errors.New("StaleClusterTime, code 209")
	CannotVerifyAndSignLogicalTime                              = errors.New("CannotVerifyAndSignLogicalTime, code 210")
	KeyNotFound                                                 = errors.New("KeyNotFound, code 211")
	IncompatibleRollbackAlgorithm                               = errors.New("IncompatibleRollbackAlgorithm, code 212")
	DuplicateSession                                            = errors.New("DuplicateSession, code 213")
	AuthenticationRestrictionUnmet                              = errors.New("AuthenticationRestrictionUnmet, code 214")
	DatabaseDropPending                                         = errors.New("DatabaseDropPending, code 215")
	ElectionInProgress                                          = errors.New("ElectionInProgress, code 216")
	IncompleteTransactionHistory                                = errors.New("IncompleteTransactionHistory, code 217")
	UpdateOperationFailed                                       = errors.New("UpdateOperationFailed, code 218")
	FTDCPathNotSet                                              = errors.New("FTDCPathNotSet, code 219")
	FTDCPathAlreadySet                                          = errors.New("FTDCPathAlreadySet, code 220")
	IndexModified                                               = errors.New("IndexModified, code 221")
	CloseChangeStream                                           = errors.New("CloseChangeStream, code 222")
	IllegalOpMsgFlag                                            = errors.New("IllegalOpMsgFlag, code 223")
	QueryFeatureNotAllowed                                      = errors.New("QueryFeatureNotAllowed, code 224")
	TransactionTooOld                                           = errors.New("TransactionTooOld, code 225")
	AtomicityFailure                                            = errors.New("AtomicityFailure, code 226")
	CannotImplicitlyCreateCollection                            = errors.New("CannotImplicitlyCreateCollection, code 227")
	SessionTransferIncomplete                                   = errors.New("SessionTransferIncomplete, code 228")
	MustDowngrade                                               = errors.New("MustDowngrade, code 229")
	DNSHostNotFound                                             = errors.New("DNSHostNotFound, code 230")
	DNSProtocolError                                            = errors.New("DNSProtocolError, code 231")
	MaxSubPipelineDepthExceeded                                 = errors.New("MaxSubPipelineDepthExceeded, code 232")
	TooManyDocumentSequences                                    = errors.New("TooManyDocumentSequences, code 233")
	RetryChangeStream                                           = errors.New("RetryChangeStream, code 234")
	InternalErrorNotSupported                                   = errors.New("InternalErrorNotSupported, code 235")
	ForTestingErrorExtraInfo                                    = errors.New("ForTestingErrorExtraInfo, code 236")
	CursorKilled                                                = errors.New("CursorKilled, code 237")
	NotImplemented                                              = errors.New("NotImplemented, code 238")
	SnapshotTooOld                                              = errors.New("SnapshotTooOld, code 239")
	DNSRecordTypeMismatch                                       = errors.New("DNSRecordTypeMismatch, code 240")
	ConversionFailure                                           = errors.New("ConversionFailure, code 241")
	CannotCreateCollection                                      = errors.New("CannotCreateCollection, code 242")
	IncompatibleWithUpgradedServer                              = errors.New("IncompatibleWithUpgradedServer, code 243")
	BrokenPromise                                               = errors.New("BrokenPromise, code 245")
	SnapshotUnavailable                                         = errors.New("SnapshotUnavailable, code 246")
	ProducerConsumerQueueBatchTooLarge                          = errors.New("ProducerConsumerQueueBatchTooLarge, code 247")
	ProducerConsumerQueueEndClosed                              = errors.New("ProducerConsumerQueueEndClosed, code 248")
	StaleDbVersion                                              = errors.New("StaleDbVersion, code 249")
	StaleChunkHistory                                           = errors.New("StaleChunkHistory, code 250")
	NoSuchTransaction                                           = errors.New("NoSuchTransaction, code 251")
	ReentrancyNotAllowed                                        = errors.New("ReentrancyNotAllowed, code 252")
	FreeMonHttpInFlight                                         = errors.New("FreeMonHttpInFlight, code 253")
	FreeMonHttpTemporaryFailure                                 = errors.New("FreeMonHttpTemporaryFailure, code 254")
	FreeMonHttpPermanentFailure                                 = errors.New("FreeMonHttpPermanentFailure, code 255")
	TransactionCommitted                                        = errors.New("TransactionCommitted, code 256")
	TransactionTooLarge                                         = errors.New("TransactionTooLarge, code 257")
	UnknownFeatureCompatibilityVersion                          = errors.New("UnknownFeatureCompatibilityVersion, code 258")
	KeyedExecutorRetry                                          = errors.New("KeyedExecutorRetry, code 259")
	InvalidResumeToken                                          = errors.New("InvalidResumeToken, code 260")
	TooManyLogicalSessions                                      = errors.New("TooManyLogicalSessions, code 261")
	ExceededTimeLimit                                           = errors.New("ExceededTimeLimit, code 262")
	OperationNotSupportedInTransaction                          = errors.New("OperationNotSupportedInTransaction, code 263")
	TooManyFilesOpen                                            = errors.New("TooManyFilesOpen, code 264")
	OrphanedRangeCleanUpFailed                                  = errors.New("OrphanedRangeCleanUpFailed, code 265")
	FailPointSetFailed                                          = errors.New("FailPointSetFailed, code 266")
	PreparedTransactionInProgress                               = errors.New("PreparedTransactionInProgress, code 267")
	CannotBackup                                                = errors.New("CannotBackup, code 268")
	DataModifiedByRepair                                        = errors.New("DataModifiedByRepair, code 269")
	RepairedReplicaSetNode                                      = errors.New("RepairedReplicaSetNode, code 270")
	JSInterpreterFailureWithStack                               = errors.New("JSInterpreterFailureWithStack, code 271")
	MigrationConflict                                           = errors.New("MigrationConflict, code 272")
	ProducerConsumerQueueProducerQueueDepthExceeded             = errors.New("ProducerConsumerQueueProducerQueueDepthExceeded, code 273")
	ProducerConsumerQueueConsumed                               = errors.New("ProducerConsumerQueueConsumed, code 274")
	ExchangePassthrough                                         = errors.New("ExchangePassthrough, code 275")
	IndexBuildAborted                                           = errors.New("IndexBuildAborted, code 276")
	AlarmAlreadyFulfilled                                       = errors.New("AlarmAlreadyFulfilled, code 277")
	UnsatisfiableCommitQuorum                                   = errors.New("UnsatisfiableCommitQuorum, code 278")
	ClientDisconnect                                            = errors.New("ClientDisconnect, code 279")
	ChangeStreamFatalError                                      = errors.New("ChangeStreamFatalError, code 280")
	TransactionCoordinatorSteppingDown                          = errors.New("TransactionCoordinatorSteppingDown, code 281")
	TransactionCoordinatorReachedAbortDecision                  = errors.New("TransactionCoordinatorReachedAbortDecision, code 282")
	WouldChangeOwningShard                                      = errors.New("WouldChangeOwningShard, code 283")
	ForTestingErrorExtraInfoWithExtraInfoInNamespace            = errors.New("ForTestingErrorExtraInfoWithExtraInfoInNamespace, code 284")
	IndexBuildAlreadyInProgress                                 = errors.New("IndexBuildAlreadyInProgress, code 285")
	ChangeStreamHistoryLost                                     = errors.New("ChangeStreamHistoryLost, code 286")
	TransactionCoordinatorDeadlineTaskCanceled                  = errors.New("TransactionCoordinatorDeadlineTaskCanceled, code 287")
	ChecksumMismatch                                            = errors.New("ChecksumMismatch, code 288")
	WaitForMajorityServiceEarlierOpTimeAvailable                = errors.New("WaitForMajorityServiceEarlierOpTimeAvailable, code 289")
	TransactionExceededLifetimeLimitSeconds                     = errors.New("TransactionExceededLifetimeLimitSeconds, code 290")
	NoQueryExecutionPlans                                       = errors.New("NoQueryExecutionPlans, code 291")
	QueryExceededMemoryLimitNoDiskUseAllowed                    = errors.New("QueryExceededMemoryLimitNoDiskUseAllowed, code 292")
	InvalidSeedList                                             = errors.New("InvalidSeedList, code 293")
	InvalidTopologyType                                         = errors.New("InvalidTopologyType, code 294")
	InvalidHeartBeatFrequency                                   = errors.New("InvalidHeartBeatFrequency, code 295")
	TopologySetNameRequired                                     = errors.New("TopologySetNameRequired, code 296")
	HierarchicalAcquisitionLevelViolation                       = errors.New("HierarchicalAcquisitionLevelViolation, code 297")
	InvalidServerType                                           = errors.New("InvalidServerType, code 298")
	OCSPCertificateStatusRevoked                                = errors.New("OCSPCertificateStatusRevoked, code 299")
	RangeDeletionAbandonedBecauseCollectionWithUUIDDoesNotExist = errors.New("RangeDeletionAbandonedBecauseCollectionWithUUIDDoesNotExist, code 300")
	DataCorruptionDetected                                      = errors.New("DataCorruptionDetected, code 301")
	OCSPCertificateStatusUnknown                                = errors.New("OCSPCertificateStatusUnknown, code 302")
	SplitHorizonChange                                          = errors.New("SplitHorizonChange, code 303")
	ShardInvalidatedForTargeting                                = errors.New("ShardInvalidatedForTargeting, code 304")
	RangeDeletionAbandonedBecauseTaskDocumentDoesNotExist       = errors.New("RangeDeletionAbandonedBecauseTaskDocumentDoesNotExist, code 307")
	CurrentConfigNotCommittedYet                                = errors.New("CurrentConfigNotCommittedYet, code 308")
	ExhaustCommandFinished                                      = errors.New("ExhaustCommandFinished, code 309")
	PeriodicJobIsStopped                                        = errors.New("PeriodicJobIsStopped, code 310")
	TransactionCoordinatorCanceled                              = errors.New("TransactionCoordinatorCanceled, code 311")
	OperationIsKilledAndDelisted                                = errors.New("OperationIsKilledAndDelisted, code 312")
	ResumableRangeDeleterDisabled                               = errors.New("ResumableRangeDeleterDisabled, code 313")
	ObjectIsBusy                                                = errors.New("ObjectIsBusy, code 314")
	TooStaleToSyncFromSource                                    = errors.New("TooStaleToSyncFromSource, code 315")
	QueryTrialRunCompleted                                      = errors.New("QueryTrialRunCompleted, code 316")
	ConnectionPoolExpired                                       = errors.New("ConnectionPoolExpired, code 317")
	ForTestingOptionalErrorExtraInfo                            = errors.New("ForTestingOptionalErrorExtraInfo, code 318")
	MovePrimaryInProgress                                       = errors.New("MovePrimaryInProgress, code 319")
	TenantMigrationConflict                                     = errors.New("TenantMigrationConflict, code 320")
	TenantMigrationCommitted                                    = errors.New("TenantMigrationCommitted, code 321")
	APIVersionError                                             = errors.New("APIVersionError, code 322")
	APIStrictError                                              = errors.New("APIStrictError, code 323")
	APIDeprecationError                                         = errors.New("APIDeprecationError, code 324")
	TenantMigrationAborted                                      = errors.New("TenantMigrationAborted, code 325")
	OplogQueryMinTsMissing                                      = errors.New("OplogQueryMinTsMissing, code 326")
	NoSuchTenantMigration                                       = errors.New("NoSuchTenantMigration, code 327")
	TenantMigrationAccessBlockerShuttingDown                    = errors.New("TenantMigrationAccessBlockerShuttingDown, code 328")
	TenantMigrationInProgress                                   = errors.New("TenantMigrationInProgress, code 329")
	SkipCommandExecution                                        = errors.New("SkipCommandExecution, code 330")
	FailedToRunWithReplyBuilder                                 = errors.New("FailedToRunWithReplyBuilder, code 331")
	CannotDowngrade                                             = errors.New("CannotDowngrade, code 332")
	ServiceExecutorInShutdown                                   = errors.New("ServiceExecutorInShutdown, code 333")
	MechanismUnavailable                                        = errors.New("MechanismUnavailable, code 334")
	TenantMigrationForgotten                                    = errors.New("TenantMigrationForgotten, code 335")
	SocketException                                             = errors.New("SocketException, code 9001")
	CannotGrowDocumentInCappedNamespace                         = errors.New("CannotGrowDocumentInCappedNamespace, code 10003")
	NotWritablePrimary                                          = errors.New("NotWritablePrimary, code 10107")
	BSONObjectTooLarge                                          = errors.New("BSONObjectTooLarge, code 10334")
	DuplicateKey                                                = errors.New("DuplicateKey, code 11000")
	InterruptedAtShutdown                                       = errors.New("InterruptedAtShutdown, code 11600")
	Interrupted                                                 = errors.New("Interrupted, code 11601")
	InterruptedDueToReplStateChange                             = errors.New("InterruptedDueToReplStateChange, code 11602")
	BackgroundOperationInProgressForDatabase                    = errors.New("BackgroundOperationInProgressForDatabase, code 12586")
	BackgroundOperationInProgressForNamespace                   = errors.New("BackgroundOperationInProgressForNamespace, code 12587")
	MergeStageNoMatchingDocument                                = errors.New("MergeStageNoMatchingDocument, code 13113")
	DatabaseDifferCase                                          = errors.New("DatabaseDifferCase, code 13297")
	StaleConfig                                                 = errors.New("StaleConfig, code 13388")
	NotPrimaryNoSecondaryOk                                     = errors.New("NotPrimaryNoSecondaryOk, code 13435")
	NotPrimaryOrSecondary                                       = errors.New("NotPrimaryOrSecondary, code 13436")
	OutOfDiskSpace                                              = errors.New("OutOfDiskSpace, code 14031")
	ClientMarkedKilled                                          = errors.New("ClientMarkedKilled, code 46841")
)

var errorMap = map[int32]error{
	1:     InternalError,
	2:     BadValue,
	4:     NoSuchKey,
	5:     GraphContainsCycle,
	6:     HostUnreachable,
	7:     HostNotFound,
	8:     UnknownError,
	9:     FailedToParse,
	10:    CannotMutateObject,
	11:    UserNotFound,
	12:    UnsupportedFormat,
	13:    Unauthorized,
	14:    TypeMismatch,
	15:    Overflow,
	16:    InvalidLength,
	17:    ProtocolError,
	18:    AuthenticationFailed,
	19:    CannotReuseObject,
	20:    IllegalOperation,
	21:    EmptyArrayOperation,
	22:    InvalidBSON,
	23:    AlreadyInitialized,
	24:    LockTimeout,
	25:    RemoteValidationError,
	26:    NamespaceNotFound,
	27:    IndexNotFound,
	28:    PathNotViable,
	29:    NonExistentPath,
	30:    InvalidPath,
	31:    RoleNotFound,
	32:    RolesNotRelated,
	33:    PrivilegeNotFound,
	34:    CannotBackfillArray,
	35:    UserModificationFailed,
	36:    RemoteChangeDetected,
	37:    FileRenameFailed,
	38:    FileNotOpen,
	39:    FileStreamFailed,
	40:    ConflictingUpdateOperators,
	41:    FileAlreadyOpen,
	42:    LogWriteFailed,
	43:    CursorNotFound,
	44:    DuplicateKey,
	59:    CommandNotFound,
	61:    ShardKeyNotFound,
	62:    OplogOperationUnsupported,
	63:    StaleShardVersion,
	64:    WriteConcernFailed,
	65:    MultipleErrorsOccurred,
	66:    ImmutableField,
	67:    CannotCreateIndex,
	68:    IndexAlreadyExists,
	69:    AuthSchemaIncompatible,
	70:    ShardNotFound,
	71:    ReplicaSetNotFound,
	72:    InvalidOptions,
	73:    InvalidNamespace,
	74:    NodeNotFound,
	75:    WriteConcernLegacyOK,
	76:    NoReplicationEnabled,
	77:    OperationIncomplete,
	78:    CommandResultSchemaViolation,
	79:    UnknownReplWriteConcern,
	80:    RoleDataInconsistent,
	81:    NoMatchParseContext,
	82:    NoProgressMade,
	83:    RemoteResultsUnavailable,
	85:    IndexOptionsConflict,
	86:    IndexKeySpecsConflict,
	87:    CannotSplit,
	89:    NetworkTimeout,
	90:    CallbackCanceled,
	91:    ShutdownInProgress,
	92:    SecondaryAheadOfPrimary,
	93:    InvalidReplicaSetConfig,
	94:    NotYetInitialized,
	95:    NotSecondary,
	96:    OperationFailed,
	97:    NoProjectionFound,
	98:    DBPathInUse,
	100:   UnsatisfiableWriteConcern,
	101:   OutdatedClient,
	102:   IncompatibleAuditMetadata,
	103:   NewReplicaSetConfigurationIncompatible,
	104:   NodeNotElectable,
	105:   IncompatibleShardingMetadata,
	106:   DistributedClockSkewed,
	107:   LockFailed,
	108:   InconsistentReplicaSetNames,
	109:   ConfigurationInProgress,
	110:   CannotInitializeNodeWithData,
	111:   NotExactValueField,
	112:   WriteConflict,
	113:   InitialSyncFailure,
	114:   InitialSyncOplogSourceMissing,
	115:   CommandNotSupported,
	116:   DocTooLargeForCapped,
	117:   ConflictingOperationInProgress,
	118:   NamespaceNotSharded,
	119:   InvalidSyncSource,
	120:   OplogStartMissing,
	121:   DocumentValidationFailure,
	123:   NotAReplicaSet,
	124:   IncompatibleElectionProtocol,
	125:   CommandFailed,
	126:   RPCProtocolNegotiationFailed,
	127:   UnrecoverableRollbackError,
	128:   LockNotFound,
	129:   LockStateChangeFailed,
	130:   SymbolNotFound,
	133:   FailedToSatisfyReadPreference,
	134:   ReadConcernMajorityNotAvailableYet,
	135:   StaleTerm,
	136:   CappedPositionLost,
	137:   IncompatibleShardingConfigVersion,
	138:   RemoteOplogStale,
	139:   JSInterpreterFailure,
	140:   InvalidSSLConfiguration,
	141:   SSLHandshakeFailed,
	142:   JSUncatchableError,
	143:   CursorInUse,
	144:   IncompatibleCatalogManager,
	145:   PooledConnectionsDropped,
	146:   ExceededMemoryLimit,
	147:   ZLibError,
	148:   ReadConcernMajorityNotEnabled,
	149:   NoConfigPrimary,
	150:   StaleEpoch,
	151:   OperationCannotBeBatched,
	152:   OplogOutOfOrder,
	153:   ChunkTooBig,
	154:   InconsistentShardIdentity,
	155:   CannotApplyOplogWhilePrimary,
	157:   CanRepairToDowngrade,
	158:   MustUpgrade,
	159:   DurationOverflow,
	160:   MaxStalenessOutOfRange,
	161:   IncompatibleCollationVersion,
	162:   CollectionIsEmpty,
	163:   ZoneStillInUse,
	164:   InitialSyncActive,
	165:   ViewDepthLimitExceeded,
	166:   CommandNotSupportedOnView,
	167:   OptionNotSupportedOnView,
	168:   InvalidPipelineOperator,
	169:   CommandOnShardedViewNotSupportedOnMongod,
	170:   TooManyMatchingDocuments,
	171:   CannotIndexParallelArrays,
	172:   TransportSessionClosed,
	173:   TransportSessionNotFound,
	174:   TransportSessionUnknown,
	175:   QueryPlanKilled,
	176:   FileOpenFailed,
	177:   ZoneNotFound,
	178:   RangeOverlapConflict,
	179:   WindowsPdhError,
	180:   BadPerfCounterPath,
	181:   AmbiguousIndexKeyPattern,
	182:   InvalidViewDefinition,
	183:   ClientMetadataMissingField,
	184:   ClientMetadataAppNameTooLarge,
	185:   ClientMetadataDocumentTooLarge,
	186:   ClientMetadataCannotBeMutated,
	187:   LinearizableReadConcernError,
	188:   IncompatibleServerVersion,
	189:   PrimarySteppedDown,
	190:   MasterSlaveConnectionFailure,
	192:   FailPointEnabled,
	193:   NoShardingEnabled,
	194:   BalancerInterrupted,
	195:   ViewPipelineMaxSizeExceeded,
	197:   InvalidIndexSpecificationOption,
	199:   ReplicaSetMonitorRemoved,
	200:   ChunkRangeCleanupPending,
	201:   CannotBuildIndexKeys,
	202:   NetworkInterfaceExceededTimeLimit,
	203:   ShardingStateNotInitialized,
	204:   TimeProofMismatch,
	205:   ClusterTimeFailsRateLimiter,
	206:   NoSuchSession,
	207:   InvalidUUID,
	208:   TooManyLocks,
	209:   StaleClusterTime,
	210:   CannotVerifyAndSignLogicalTime,
	211:   KeyNotFound,
	212:   IncompatibleRollbackAlgorithm,
	213:   DuplicateSession,
	214:   AuthenticationRestrictionUnmet,
	215:   DatabaseDropPending,
	216:   ElectionInProgress,
	217:   IncompleteTransactionHistory,
	218:   UpdateOperationFailed,
	219:   FTDCPathNotSet,
	220:   FTDCPathAlreadySet,
	221:   IndexModified,
	222:   CloseChangeStream,
	223:   IllegalOpMsgFlag,
	224:   QueryFeatureNotAllowed,
	225:   TransactionTooOld,
	226:   AtomicityFailure,
	227:   CannotImplicitlyCreateCollection,
	228:   SessionTransferIncomplete,
	229:   MustDowngrade,
	230:   DNSHostNotFound,
	231:   DNSProtocolError,
	232:   MaxSubPipelineDepthExceeded,
	233:   TooManyDocumentSequences,
	234:   RetryChangeStream,
	235:   InternalErrorNotSupported,
	236:   ForTestingErrorExtraInfo,
	237:   CursorKilled,
	238:   NotImplemented,
	239:   SnapshotTooOld,
	240:   DNSRecordTypeMismatch,
	241:   ConversionFailure,
	242:   CannotCreateCollection,
	243:   IncompatibleWithUpgradedServer,
	245:   BrokenPromise,
	246:   SnapshotUnavailable,
	247:   ProducerConsumerQueueBatchTooLarge,
	248:   ProducerConsumerQueueEndClosed,
	249:   StaleDbVersion,
	250:   StaleChunkHistory,
	251:   NoSuchTransaction,
	252:   ReentrancyNotAllowed,
	253:   FreeMonHttpInFlight,
	254:   FreeMonHttpTemporaryFailure,
	255:   FreeMonHttpPermanentFailure,
	256:   TransactionCommitted,
	257:   TransactionTooLarge,
	258:   UnknownFeatureCompatibilityVersion,
	259:   KeyedExecutorRetry,
	260:   InvalidResumeToken,
	261:   TooManyLogicalSessions,
	262:   ExceededTimeLimit,
	263:   OperationNotSupportedInTransaction,
	264:   TooManyFilesOpen,
	265:   OrphanedRangeCleanUpFailed,
	266:   FailPointSetFailed,
	267:   PreparedTransactionInProgress,
	268:   CannotBackup,
	269:   DataModifiedByRepair,
	270:   RepairedReplicaSetNode,
	271:   JSInterpreterFailureWithStack,
	272:   MigrationConflict,
	273:   ProducerConsumerQueueProducerQueueDepthExceeded,
	274:   ProducerConsumerQueueConsumed,
	275:   ExchangePassthrough,
	276:   IndexBuildAborted,
	277:   AlarmAlreadyFulfilled,
	278:   UnsatisfiableCommitQuorum,
	279:   ClientDisconnect,
	280:   ChangeStreamFatalError,
	281:   TransactionCoordinatorSteppingDown,
	282:   TransactionCoordinatorReachedAbortDecision,
	283:   WouldChangeOwningShard,
	284:   ForTestingErrorExtraInfoWithExtraInfoInNamespace,
	285:   IndexBuildAlreadyInProgress,
	286:   ChangeStreamHistoryLost,
	287:   TransactionCoordinatorDeadlineTaskCanceled,
	288:   ChecksumMismatch,
	289:   WaitForMajorityServiceEarlierOpTimeAvailable,
	290:   TransactionExceededLifetimeLimitSeconds,
	291:   NoQueryExecutionPlans,
	292:   QueryExceededMemoryLimitNoDiskUseAllowed,
	293:   InvalidSeedList,
	294:   InvalidTopologyType,
	295:   InvalidHeartBeatFrequency,
	296:   TopologySetNameRequired,
	297:   HierarchicalAcquisitionLevelViolation,
	298:   InvalidServerType,
	299:   OCSPCertificateStatusRevoked,
	300:   RangeDeletionAbandonedBecauseCollectionWithUUIDDoesNotExist,
	301:   DataCorruptionDetected,
	302:   OCSPCertificateStatusUnknown,
	303:   SplitHorizonChange,
	304:   ShardInvalidatedForTargeting,
	307:   RangeDeletionAbandonedBecauseTaskDocumentDoesNotExist,
	308:   CurrentConfigNotCommittedYet,
	309:   ExhaustCommandFinished,
	310:   PeriodicJobIsStopped,
	311:   TransactionCoordinatorCanceled,
	312:   OperationIsKilledAndDelisted,
	313:   ResumableRangeDeleterDisabled,
	314:   ObjectIsBusy,
	315:   TooStaleToSyncFromSource,
	316:   QueryTrialRunCompleted,
	317:   ConnectionPoolExpired,
	318:   ForTestingOptionalErrorExtraInfo,
	319:   MovePrimaryInProgress,
	320:   TenantMigrationConflict,
	321:   TenantMigrationCommitted,
	322:   APIVersionError,
	323:   APIStrictError,
	324:   APIDeprecationError,
	325:   TenantMigrationAborted,
	326:   OplogQueryMinTsMissing,
	327:   NoSuchTenantMigration,
	328:   TenantMigrationAccessBlockerShuttingDown,
	329:   TenantMigrationInProgress,
	330:   SkipCommandExecution,
	331:   FailedToRunWithReplyBuilder,
	332:   CannotDowngrade,
	333:   ServiceExecutorInShutdown,
	334:   MechanismUnavailable,
	335:   TenantMigrationForgotten,
	9001:  SocketException,
	10003: CannotGrowDocumentInCappedNamespace,
	10107: NotWritablePrimary,
	10334: BSONObjectTooLarge,
	11000: DuplicateKey,
	11600: InterruptedAtShutdown,
	11601: Interrupted,
	11602: InterruptedDueToReplStateChange,
	12586: BackgroundOperationInProgressForDatabase,
	12587: BackgroundOperationInProgressForNamespace,
	13113: MergeStageNoMatchingDocument,
	13297: DatabaseDifferCase,
	13388: StaleConfig,
	13435: NotPrimaryNoSecondaryOk,
	13436: NotPrimaryOrSecondary,
	14031: OutOfDiskSpace,
	46841: ClientMarkedKilled,
}

var mu sync.RWMutex

func ErrorFromCode(code int32) (error, bool) {
	mu.RLock()
	defer mu.RUnlock()
	err, ok := errorMap[code]
	return err, ok
}

func handleError(err error) error {
	if err == nil {
		return nil
	}

	switch {
	case errors.Is(err, mongo.ErrNoDocuments):
		return ErrNotFound

	case mongo.IsDuplicateKeyError(err):
		return fmt.Errorf("%w: %v", ErrDuplicate, err)

	case mongo.IsNetworkError(err) ||
		errors.Is(err, mongo.ErrWrongClient) ||
		errors.Is(err, mongo.ErrClientDisconnected) ||
		errors.Is(err, mongo.ErrStreamClosed):
		return fmt.Errorf("%w: %v", ErrNetwork, err)

	case mongo.IsTimeout(err):
		return fmt.Errorf("%w: %v", ErrTimeout, err)

	case strings.Contains(err.Error(), "must be a pointer to") ||
		strings.Contains(err.Error(), "DocumentSequence"):
		return fmt.Errorf("%w: %v", ErrInvalidArgument, err)

	case errors.Is(err, mongo.ErrNilValue) ||
		errors.Is(err, mongo.ErrNilDocument) ||
		errors.Is(err, mongo.ErrEmptySlice) ||
		errors.Is(err, mongo.ErrFileNotFound) ||
		errors.Is(err, mongo.ErrInvalidIndexValue) ||
		errors.Is(err, mongo.ErrMultipleIndexDrop) ||
		errors.Is(err, mongo.ErrNonStringIndexName) ||
		errors.Is(err, mongo.ErrNotSlice) ||
		errors.Is(err, mongo.ErrStreamClosed) ||
		errors.Is(err, mongo.ErrWrongClient):
		return fmt.Errorf("%w: %v", ErrInvalidArgument, err)

	case errors.Is(err, mongo.ErrMissingChunk) ||
		errors.Is(err, mongo.ErrMissingGridFSChunkSize) ||
		errors.Is(err, mongo.ErrWrongSize) ||
		errors.Is(err, mongo.ErrNilCursor) ||
		errors.Is(err, mongo.ErrMissingResumeToken):
		return fmt.Errorf("%w: %v", ErrBadServer, err)
	}

	var e mongo.CommandError
	if errors.As(err, &e) {
		errFromCode, ok := ErrorFromCode(e.Code)
		if !ok {
			return e
		}
		return fmt.Errorf("%w: %v", errFromCode, e)
	}

	var writeError mongo.WriteException
	if errors.As(err, &writeError) {
		var errs []error
		for _, we := range writeError.WriteErrors {
			errFromCode, ok := ErrorFromCode(int32(we.Code))
			if !ok {
				errs = append(errs, we)
				continue
			}
			errs = append(errs, fmt.Errorf("%w: %v", errFromCode, we))
		}
		return errors.Join(errs...)
	}

	var marshalError mongo.MarshalError
	if errors.As(err, &marshalError) {
		return fmt.Errorf("%w: %s", ErrInvalidArgument, marshalError)
	}

	var mapArgError mongo.ErrMapForOrderedArgument
	if errors.As(err, &mapArgError) {
		return fmt.Errorf("%w: %s", ErrInvalidArgument, mapArgError)
	}

	var bwe mongo.BulkWriteException
	if errors.As(err, &bwe) {
		var errs []error
		for _, we := range bwe.WriteErrors {
			errFromCode, ok := ErrorFromCode(int32(we.Code))
			if !ok {
				errs = append(errs, we)
				continue
			}
			errs = append(errs, fmt.Errorf("%w: %v", errFromCode, we))
		}
		return errors.Join(errs...)
	}

	var bulkWriteError mongo.WriteConcernError
	if errors.As(err, &bulkWriteError) {
		errFromCode, ok := ErrorFromCode(int32(bulkWriteError.Code))
		if !ok {
			return fmt.Errorf("bulk write error: %w", bulkWriteError)
		}
		return fmt.Errorf("%w: %v", errFromCode, bulkWriteError)
	}

	var mongoCryptError mongo.MongocryptError
	if errors.As(err, &mongoCryptError) {
		errFromCode, ok := ErrorFromCode(int32(mongoCryptError.Code))
		if !ok {
			return fmt.Errorf("mongocrypt error: %w", mongoCryptError)
		}
		return fmt.Errorf("%w: %v", errFromCode, mongoCryptError)
	}

	return err
}
