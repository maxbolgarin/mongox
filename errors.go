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
	ErrInternalError                                               = errors.New("InternalError, code 1")
	ErrBadValue                                                    = errors.New("BadValue, code 2")
	ErrNoSuchKey                                                   = errors.New("NoSuchKey, code 4")
	ErrGraphContainsCycle                                          = errors.New("GraphContainsCycle, code 5")
	ErrHostUnreachable                                             = errors.New("HostUnreachable, code 6")
	ErrHostNotFound                                                = errors.New("HostNotFound, code 7")
	ErrUnknownError                                                = errors.New("UnknownError, code 8")
	ErrFailedToParse                                               = errors.New("FailedToParse, code 9")
	ErrCannotMutateObject                                          = errors.New("CannotMutateObject, code 10")
	ErrUserNotFound                                                = errors.New("UserNotFound, code 11")
	ErrUnsupportedFormat                                           = errors.New("UnsupportedFormat, code 12")
	ErrUnauthorized                                                = errors.New("Unauthorized, code 13")
	ErrTypeMismatch                                                = errors.New("TypeMismatch, code 14")
	ErrOverflow                                                    = errors.New("Overflow, code 15")
	ErrInvalidLength                                               = errors.New("InvalidLength, code 16")
	ErrProtocolError                                               = errors.New("ProtocolError, code 17")
	ErrAuthenticationFailed                                        = errors.New("AuthenticationFailed, code 18")
	ErrCannotReuseObject                                           = errors.New("CannotReuseObject, code 19")
	ErrIllegalOperation                                            = errors.New("IllegalOperation, code 20")
	ErrEmptyArrayOperation                                         = errors.New("EmptyArrayOperation, code 21")
	ErrInvalidBSON                                                 = errors.New("InvalidBSON, code 22")
	ErrAlreadyInitialized                                          = errors.New("AlreadyInitialized, code 23")
	ErrLockTimeout                                                 = errors.New("LockTimeout, code 24")
	ErrRemoteValidationError                                       = errors.New("RemoteValidationError, code 25")
	ErrNamespaceNotFound                                           = errors.New("NamespaceNotFound, code 26")
	ErrIndexNotFound                                               = errors.New("IndexNotFound, code 27")
	ErrPathNotViable                                               = errors.New("PathNotViable, code 28")
	ErrNonExistentPath                                             = errors.New("NonExistentPath, code 29")
	ErrInvalidPath                                                 = errors.New("InvalidPath, code 30")
	ErrRoleNotFound                                                = errors.New("RoleNotFound, code 31")
	ErrRolesNotRelated                                             = errors.New("RolesNotRelated, code 32")
	ErrPrivilegeNotFound                                           = errors.New("PrivilegeNotFound, code 33")
	ErrCannotBackfillArray                                         = errors.New("CannotBackfillArray, code 34")
	ErrUserModificationFailed                                      = errors.New("UserModificationFailed, code 35")
	ErrRemoteChangeDetected                                        = errors.New("RemoteChangeDetected, code 36")
	ErrFileRenameFailed                                            = errors.New("FileRenameFailed, code 37")
	ErrFileNotOpen                                                 = errors.New("FileNotOpen, code 38")
	ErrFileStreamFailed                                            = errors.New("FileStreamFailed, code 39")
	ErrConflictingUpdateOperators                                  = errors.New("ConflictingUpdateOperators, code 40")
	ErrFileAlreadyOpen                                             = errors.New("FileAlreadyOpen, code 41")
	ErrLogWriteFailed                                              = errors.New("LogWriteFailed, code 42")
	ErrCursorNotFound                                              = errors.New("CursorNotFound, code 43")
	ErrUserDataInconsistent                                        = errors.New("UserDataInconsistent, code 45")
	ErrLockBusy                                                    = errors.New("LockBusy, code 46")
	ErrNoMatchingDocument                                          = errors.New("NoMatchingDocument, code 47")
	ErrNamespaceExists                                             = errors.New("NamespaceExists, code 48")
	ErrInvalidRoleModification                                     = errors.New("InvalidRoleModification, code 49")
	ErrMaxTimeMSExpired                                            = errors.New("MaxTimeMSExpired, code 50")
	ErrManualInterventionRequired                                  = errors.New("ManualInterventionRequired, code 51")
	ErrDollarPrefixedFieldName                                     = errors.New("DollarPrefixedFieldName, code 52")
	ErrInvalidIdField                                              = errors.New("InvalidIdField, code 53")
	ErrNotSingleValueField                                         = errors.New("NotSingleValueField, code 54")
	ErrInvalidDBRef                                                = errors.New("InvalidDBRef, code 55")
	ErrEmptyFieldName                                              = errors.New("EmptyFieldName, code 56")
	ErrDottedFieldName                                             = errors.New("DottedFieldName, code 57")
	ErrRoleModificationFailed                                      = errors.New("RoleModificationFailed, code 58")
	ErrCommandNotFound                                             = errors.New("CommandNotFound, code 59")
	ErrShardKeyNotFound                                            = errors.New("ShardKeyNotFound, code 61")
	ErrOplogOperationUnsupported                                   = errors.New("OplogOperationUnsupported, code 62")
	ErrStaleShardVersion                                           = errors.New("StaleShardVersion, code 63")
	ErrWriteConcernFailed                                          = errors.New("WriteConcernFailed, code 64")
	ErrMultipleErrorsOccurred                                      = errors.New("MultipleErrorsOccurred, code 65")
	ErrImmutableField                                              = errors.New("ImmutableField, code 66")
	ErrCannotCreateIndex                                           = errors.New("CannotCreateIndex, code 67")
	ErrIndexAlreadyExists                                          = errors.New("IndexAlreadyExists, code 68")
	ErrAuthSchemaIncompatible                                      = errors.New("AuthSchemaIncompatible, code 69")
	ErrShardNotFound                                               = errors.New("ShardNotFound, code 70")
	ErrReplicaSetNotFound                                          = errors.New("ReplicaSetNotFound, code 71")
	ErrInvalidOptions                                              = errors.New("InvalidOptions, code 72")
	ErrInvalidNamespace                                            = errors.New("InvalidNamespace, code 73")
	ErrNodeNotFound                                                = errors.New("NodeNotFound, code 74")
	ErrWriteConcernLegacyOK                                        = errors.New("WriteConcernLegacyOK, code 75")
	ErrNoReplicationEnabled                                        = errors.New("NoReplicationEnabled, code 76")
	ErrOperationIncomplete                                         = errors.New("OperationIncomplete, code 77")
	ErrCommandResultSchemaViolation                                = errors.New("CommandResultSchemaViolation, code 78")
	ErrUnknownReplWriteConcern                                     = errors.New("UnknownReplWriteConcern, code 79")
	ErrRoleDataInconsistent                                        = errors.New("RoleDataInconsistent, code 80")
	ErrNoMatchParseContext                                         = errors.New("NoMatchParseContext, code 81")
	ErrNoProgressMade                                              = errors.New("NoProgressMade, code 82")
	ErrRemoteResultsUnavailable                                    = errors.New("RemoteResultsUnavailable, code 83")
	ErrIndexOptionsConflict                                        = errors.New("IndexOptionsConflict, code 85")
	ErrIndexKeySpecsConflict                                       = errors.New("IndexKeySpecsConflict, code 86")
	ErrCannotSplit                                                 = errors.New("CannotSplit, code 87")
	ErrNetworkTimeout                                              = errors.New("NetworkTimeout, code 89")
	ErrCallbackCanceled                                            = errors.New("CallbackCanceled, code 90")
	ErrShutdownInProgress                                          = errors.New("ShutdownInProgress, code 91")
	ErrSecondaryAheadOfPrimary                                     = errors.New("SecondaryAheadOfPrimary, code 92")
	ErrInvalidReplicaSetConfig                                     = errors.New("InvalidReplicaSetConfig, code 93")
	ErrNotYetInitialized                                           = errors.New("NotYetInitialized, code 94")
	ErrNotSecondary                                                = errors.New("NotSecondary, code 95")
	ErrOperationFailed                                             = errors.New("OperationFailed, code 96")
	ErrNoProjectionFound                                           = errors.New("NoProjectionFound, code 97")
	ErrDBPathInUse                                                 = errors.New("DBPathInUse, code 98")
	ErrUnsatisfiableWriteConcern                                   = errors.New("UnsatisfiableWriteConcern, code 100")
	ErrOutdatedClient                                              = errors.New("OutdatedClient, code 101")
	ErrIncompatibleAuditMetadata                                   = errors.New("IncompatibleAuditMetadata, code 102")
	ErrNewReplicaSetConfigurationIncompatible                      = errors.New("NewReplicaSetConfigurationIncompatible, code 103")
	ErrNodeNotElectable                                            = errors.New("NodeNotElectable, code 104")
	ErrIncompatibleShardingMetadata                                = errors.New("IncompatibleShardingMetadata, code 105")
	ErrDistributedClockSkewed                                      = errors.New("DistributedClockSkewed, code 106")
	ErrLockFailed                                                  = errors.New("LockFailed, code 107")
	ErrInconsistentReplicaSetNames                                 = errors.New("InconsistentReplicaSetNames, code 108")
	ErrConfigurationInProgress                                     = errors.New("ConfigurationInProgress, code 109")
	ErrCannotInitializeNodeWithData                                = errors.New("CannotInitializeNodeWithData, code 110")
	ErrNotExactValueField                                          = errors.New("NotExactValueField, code 111")
	ErrWriteConflict                                               = errors.New("WriteConflict, code 112")
	ErrInitialSyncFailure                                          = errors.New("InitialSyncFailure, code 113")
	ErrInitialSyncOplogSourceMissing                               = errors.New("InitialSyncOplogSourceMissing, code 114")
	ErrCommandNotSupported                                         = errors.New("CommandNotSupported, code 115")
	ErrDocTooLargeForCapped                                        = errors.New("DocTooLargeForCapped, code 116")
	ErrConflictingOperationInProgress                              = errors.New("ConflictingOperationInProgress, code 117")
	ErrNamespaceNotSharded                                         = errors.New("NamespaceNotSharded, code 118")
	ErrInvalidSyncSource                                           = errors.New("InvalidSyncSource, code 119")
	ErrOplogStartMissing                                           = errors.New("OplogStartMissing, code 120")
	ErrDocumentValidationFailure                                   = errors.New("DocumentValidationFailure, code 121")
	ErrNotAReplicaSet                                              = errors.New("NotAReplicaSet, code 123")
	ErrIncompatibleElectionProtocol                                = errors.New("IncompatibleElectionProtocol, code 124")
	ErrCommandFailed                                               = errors.New("CommandFailed, code 125")
	ErrRPCProtocolNegotiationFailed                                = errors.New("RPCProtocolNegotiationFailed, code 126")
	ErrUnrecoverableRollbackError                                  = errors.New("UnrecoverableRollbackError, code 127")
	ErrLockNotFound                                                = errors.New("LockNotFound, code 128")
	ErrLockStateChangeFailed                                       = errors.New("LockStateChangeFailed, code 129")
	ErrSymbolNotFound                                              = errors.New("SymbolNotFound, code 130")
	ErrFailedToSatisfyReadPreference                               = errors.New("FailedToSatisfyReadPreference, code 133")
	ErrReadConcernMajorityNotAvailableYet                          = errors.New("ReadConcernMajorityNotAvailableYet, code 134")
	ErrStaleTerm                                                   = errors.New("StaleTerm, code 135")
	ErrCappedPositionLost                                          = errors.New("CappedPositionLost, code 136")
	ErrIncompatibleShardingConfigVersion                           = errors.New("IncompatibleShardingConfigVersion, code 137")
	ErrRemoteOplogStale                                            = errors.New("RemoteOplogStale, code 138")
	ErrJSInterpreterFailure                                        = errors.New("JSInterpreterFailure, code 139")
	ErrInvalidSSLConfiguration                                     = errors.New("InvalidSSLConfiguration, code 140")
	ErrSSLHandshakeFailed                                          = errors.New("SSLHandshakeFailed, code 141")
	ErrJSUncatchableError                                          = errors.New("JSUncatchableError, code 142")
	ErrCursorInUse                                                 = errors.New("CursorInUse, code 143")
	ErrIncompatibleCatalogManager                                  = errors.New("IncompatibleCatalogManager, code 144")
	ErrPooledConnectionsDropped                                    = errors.New("PooledConnectionsDropped, code 145")
	ErrExceededMemoryLimit                                         = errors.New("ExceededMemoryLimit, code 146")
	ErrZLibError                                                   = errors.New("ZLibError, code 147")
	ErrReadConcernMajorityNotEnabled                               = errors.New("ReadConcernMajorityNotEnabled, code 148")
	ErrNoConfigPrimary                                             = errors.New("NoConfigPrimary, code 149")
	ErrStaleEpoch                                                  = errors.New("StaleEpoch, code 150")
	ErrOperationCannotBeBatched                                    = errors.New("OperationCannotBeBatched, code 151")
	ErrOplogOutOfOrder                                             = errors.New("OplogOutOfOrder, code 152")
	ErrChunkTooBig                                                 = errors.New("ChunkTooBig, code 153")
	ErrInconsistentShardIdentity                                   = errors.New("InconsistentShardIdentity, code 154")
	ErrCannotApplyOplogWhilePrimary                                = errors.New("CannotApplyOplogWhilePrimary, code 155")
	ErrCanRepairToDowngrade                                        = errors.New("CanRepairToDowngrade, code 157")
	ErrMustUpgrade                                                 = errors.New("MustUpgrade, code 158")
	ErrDurationOverflow                                            = errors.New("DurationOverflow, code 159")
	ErrMaxStalenessOutOfRange                                      = errors.New("MaxStalenessOutOfRange, code 160")
	ErrIncompatibleCollationVersion                                = errors.New("IncompatibleCollationVersion, code 161")
	ErrCollectionIsEmpty                                           = errors.New("CollectionIsEmpty, code 162")
	ErrZoneStillInUse                                              = errors.New("ZoneStillInUse, code 163")
	ErrInitialSyncActive                                           = errors.New("InitialSyncActive, code 164")
	ErrViewDepthLimitExceeded                                      = errors.New("ViewDepthLimitExceeded, code 165")
	ErrCommandNotSupportedOnView                                   = errors.New("CommandNotSupportedOnView, code 166")
	ErrOptionNotSupportedOnView                                    = errors.New("OptionNotSupportedOnView, code 167")
	ErrInvalidPipelineOperator                                     = errors.New("InvalidPipelineOperator, code 168")
	ErrCommandOnShardedViewNotSupportedOnMongod                    = errors.New("CommandOnShardedViewNotSupportedOnMongod, code 169")
	ErrTooManyMatchingDocuments                                    = errors.New("TooManyMatchingDocuments, code 170")
	ErrCannotIndexParallelArrays                                   = errors.New("CannotIndexParallelArrays, code 171")
	ErrTransportSessionClosed                                      = errors.New("TransportSessionClosed, code 172")
	ErrTransportSessionNotFound                                    = errors.New("TransportSessionNotFound, code 173")
	ErrTransportSessionUnknown                                     = errors.New("TransportSessionUnknown, code 174")
	ErrQueryPlanKilled                                             = errors.New("QueryPlanKilled, code 175")
	ErrFileOpenFailed                                              = errors.New("FileOpenFailed, code 176")
	ErrZoneNotFound                                                = errors.New("ZoneNotFound, code 177")
	ErrRangeOverlapConflict                                        = errors.New("RangeOverlapConflict, code 178")
	ErrWindowsPdhError                                             = errors.New("WindowsPdhError, code 179")
	ErrBadPerfCounterPath                                          = errors.New("BadPerfCounterPath, code 180")
	ErrAmbiguousIndexKeyPattern                                    = errors.New("AmbiguousIndexKeyPattern, code 181")
	ErrInvalidViewDefinition                                       = errors.New("InvalidViewDefinition, code 182")
	ErrClientMetadataMissingField                                  = errors.New("ClientMetadataMissingField, code 183")
	ErrClientMetadataAppNameTooLarge                               = errors.New("ClientMetadataAppNameTooLarge, code 184")
	ErrClientMetadataDocumentTooLarge                              = errors.New("ClientMetadataDocumentTooLarge, code 185")
	ErrClientMetadataCannotBeMutated                               = errors.New("ClientMetadataCannotBeMutated, code 186")
	ErrLinearizableReadConcernError                                = errors.New("LinearizableReadConcernError, code 187")
	ErrIncompatibleServerVersion                                   = errors.New("IncompatibleServerVersion, code 188")
	ErrPrimarySteppedDown                                          = errors.New("PrimarySteppedDown, code 189")
	ErrMasterSlaveConnectionFailure                                = errors.New("MasterSlaveConnectionFailure, code 190")
	ErrFailPointEnabled                                            = errors.New("FailPointEnabled, code 192")
	ErrNoShardingEnabled                                           = errors.New("NoShardingEnabled, code 193")
	ErrBalancerInterrupted                                         = errors.New("BalancerInterrupted, code 194")
	ErrViewPipelineMaxSizeExceeded                                 = errors.New("ViewPipelineMaxSizeExceeded, code 195")
	ErrInvalidIndexSpecificationOption                             = errors.New("InvalidIndexSpecificationOption, code 197")
	ErrReplicaSetMonitorRemoved                                    = errors.New("ReplicaSetMonitorRemoved, code 199")
	ErrChunkRangeCleanupPending                                    = errors.New("ChunkRangeCleanupPending, code 200")
	ErrCannotBuildIndexKeys                                        = errors.New("CannotBuildIndexKeys, code 201")
	ErrNetworkInterfaceExceededTimeLimit                           = errors.New("NetworkInterfaceExceededTimeLimit, code 202")
	ErrShardingStateNotInitialized                                 = errors.New("ShardingStateNotInitialized, code 203")
	ErrTimeProofMismatch                                           = errors.New("TimeProofMismatch, code 204")
	ErrClusterTimeFailsRateLimiter                                 = errors.New("ClusterTimeFailsRateLimiter, code 205")
	ErrNoSuchSession                                               = errors.New("NoSuchSession, code 206")
	ErrInvalidUUID                                                 = errors.New("InvalidUUID, code 207")
	ErrTooManyLocks                                                = errors.New("TooManyLocks, code 208")
	ErrStaleClusterTime                                            = errors.New("StaleClusterTime, code 209")
	ErrCannotVerifyAndSignLogicalTime                              = errors.New("CannotVerifyAndSignLogicalTime, code 210")
	ErrKeyNotFound                                                 = errors.New("KeyNotFound, code 211")
	ErrIncompatibleRollbackAlgorithm                               = errors.New("IncompatibleRollbackAlgorithm, code 212")
	ErrDuplicateSession                                            = errors.New("DuplicateSession, code 213")
	ErrAuthenticationRestrictionUnmet                              = errors.New("AuthenticationRestrictionUnmet, code 214")
	ErrDatabaseDropPending                                         = errors.New("DatabaseDropPending, code 215")
	ErrElectionInProgress                                          = errors.New("ElectionInProgress, code 216")
	ErrIncompleteTransactionHistory                                = errors.New("IncompleteTransactionHistory, code 217")
	ErrUpdateOperationFailed                                       = errors.New("UpdateOperationFailed, code 218")
	ErrFTDCPathNotSet                                              = errors.New("FTDCPathNotSet, code 219")
	ErrFTDCPathAlreadySet                                          = errors.New("FTDCPathAlreadySet, code 220")
	ErrIndexModified                                               = errors.New("IndexModified, code 221")
	ErrCloseChangeStream                                           = errors.New("CloseChangeStream, code 222")
	ErrIllegalOpMsgFlag                                            = errors.New("IllegalOpMsgFlag, code 223")
	ErrQueryFeatureNotAllowed                                      = errors.New("QueryFeatureNotAllowed, code 224")
	ErrTransactionTooOld                                           = errors.New("TransactionTooOld, code 225")
	ErrAtomicityFailure                                            = errors.New("AtomicityFailure, code 226")
	ErrCannotImplicitlyCreateCollection                            = errors.New("CannotImplicitlyCreateCollection, code 227")
	ErrSessionTransferIncomplete                                   = errors.New("SessionTransferIncomplete, code 228")
	ErrMustDowngrade                                               = errors.New("MustDowngrade, code 229")
	ErrDNSHostNotFound                                             = errors.New("DNSHostNotFound, code 230")
	ErrDNSProtocolError                                            = errors.New("DNSProtocolError, code 231")
	ErrMaxSubPipelineDepthExceeded                                 = errors.New("MaxSubPipelineDepthExceeded, code 232")
	ErrTooManyDocumentSequences                                    = errors.New("TooManyDocumentSequences, code 233")
	ErrRetryChangeStream                                           = errors.New("RetryChangeStream, code 234")
	ErrInternalErrorNotSupported                                   = errors.New("InternalErrorNotSupported, code 235")
	ErrForTestingErrorExtraInfo                                    = errors.New("ForTestingErrorExtraInfo, code 236")
	ErrCursorKilled                                                = errors.New("CursorKilled, code 237")
	ErrNotImplemented                                              = errors.New("NotImplemented, code 238")
	ErrSnapshotTooOld                                              = errors.New("SnapshotTooOld, code 239")
	ErrDNSRecordTypeMismatch                                       = errors.New("DNSRecordTypeMismatch, code 240")
	ErrConversionFailure                                           = errors.New("ConversionFailure, code 241")
	ErrCannotCreateCollection                                      = errors.New("CannotCreateCollection, code 242")
	ErrIncompatibleWithUpgradedServer                              = errors.New("IncompatibleWithUpgradedServer, code 243")
	ErrBrokenPromise                                               = errors.New("BrokenPromise, code 245")
	ErrSnapshotUnavailable                                         = errors.New("SnapshotUnavailable, code 246")
	ErrProducerConsumerQueueBatchTooLarge                          = errors.New("ProducerConsumerQueueBatchTooLarge, code 247")
	ErrProducerConsumerQueueEndClosed                              = errors.New("ProducerConsumerQueueEndClosed, code 248")
	ErrStaleDbVersion                                              = errors.New("StaleDbVersion, code 249")
	ErrStaleChunkHistory                                           = errors.New("StaleChunkHistory, code 250")
	ErrNoSuchTransaction                                           = errors.New("NoSuchTransaction, code 251")
	ErrReentrancyNotAllowed                                        = errors.New("ReentrancyNotAllowed, code 252")
	ErrFreeMonHttpInFlight                                         = errors.New("FreeMonHttpInFlight, code 253")
	ErrFreeMonHttpTemporaryFailure                                 = errors.New("FreeMonHttpTemporaryFailure, code 254")
	ErrFreeMonHttpPermanentFailure                                 = errors.New("FreeMonHttpPermanentFailure, code 255")
	ErrTransactionCommitted                                        = errors.New("TransactionCommitted, code 256")
	ErrTransactionTooLarge                                         = errors.New("TransactionTooLarge, code 257")
	ErrUnknownFeatureCompatibilityVersion                          = errors.New("UnknownFeatureCompatibilityVersion, code 258")
	ErrKeyedExecutorRetry                                          = errors.New("KeyedExecutorRetry, code 259")
	ErrInvalidResumeToken                                          = errors.New("InvalidResumeToken, code 260")
	ErrTooManyLogicalSessions                                      = errors.New("TooManyLogicalSessions, code 261")
	ErrExceededTimeLimit                                           = errors.New("ExceededTimeLimit, code 262")
	ErrOperationNotSupportedInTransaction                          = errors.New("OperationNotSupportedInTransaction, code 263")
	ErrTooManyFilesOpen                                            = errors.New("TooManyFilesOpen, code 264")
	ErrOrphanedRangeCleanUpFailed                                  = errors.New("OrphanedRangeCleanUpFailed, code 265")
	ErrFailPointSetFailed                                          = errors.New("FailPointSetFailed, code 266")
	ErrPreparedTransactionInProgress                               = errors.New("PreparedTransactionInProgress, code 267")
	ErrCannotBackup                                                = errors.New("CannotBackup, code 268")
	ErrDataModifiedByRepair                                        = errors.New("DataModifiedByRepair, code 269")
	ErrRepairedReplicaSetNode                                      = errors.New("RepairedReplicaSetNode, code 270")
	ErrJSInterpreterFailureWithStack                               = errors.New("JSInterpreterFailureWithStack, code 271")
	ErrMigrationConflict                                           = errors.New("MigrationConflict, code 272")
	ErrProducerConsumerQueueProducerQueueDepthExceeded             = errors.New("ProducerConsumerQueueProducerQueueDepthExceeded, code 273")
	ErrProducerConsumerQueueConsumed                               = errors.New("ProducerConsumerQueueConsumed, code 274")
	ErrExchangePassthrough                                         = errors.New("ExchangePassthrough, code 275")
	ErrIndexBuildAborted                                           = errors.New("IndexBuildAborted, code 276")
	ErrAlarmAlreadyFulfilled                                       = errors.New("AlarmAlreadyFulfilled, code 277")
	ErrUnsatisfiableCommitQuorum                                   = errors.New("UnsatisfiableCommitQuorum, code 278")
	ErrClientDisconnect                                            = errors.New("ClientDisconnect, code 279")
	ErrChangeStreamFatalError                                      = errors.New("ChangeStreamFatalError, code 280")
	ErrTransactionCoordinatorSteppingDown                          = errors.New("TransactionCoordinatorSteppingDown, code 281")
	ErrTransactionCoordinatorReachedAbortDecision                  = errors.New("TransactionCoordinatorReachedAbortDecision, code 282")
	ErrWouldChangeOwningShard                                      = errors.New("WouldChangeOwningShard, code 283")
	ErrForTestingErrorExtraInfoWithExtraInfoInNamespace            = errors.New("ForTestingErrorExtraInfoWithExtraInfoInNamespace, code 284")
	ErrIndexBuildAlreadyInProgress                                 = errors.New("IndexBuildAlreadyInProgress, code 285")
	ErrChangeStreamHistoryLost                                     = errors.New("ChangeStreamHistoryLost, code 286")
	ErrTransactionCoordinatorDeadlineTaskCanceled                  = errors.New("TransactionCoordinatorDeadlineTaskCanceled, code 287")
	ErrChecksumMismatch                                            = errors.New("ChecksumMismatch, code 288")
	ErrWaitForMajorityServiceEarlierOpTimeAvailable                = errors.New("WaitForMajorityServiceEarlierOpTimeAvailable, code 289")
	ErrTransactionExceededLifetimeLimitSeconds                     = errors.New("TransactionExceededLifetimeLimitSeconds, code 290")
	ErrNoQueryExecutionPlans                                       = errors.New("NoQueryExecutionPlans, code 291")
	ErrQueryExceededMemoryLimitNoDiskUseAllowed                    = errors.New("QueryExceededMemoryLimitNoDiskUseAllowed, code 292")
	ErrInvalidSeedList                                             = errors.New("InvalidSeedList, code 293")
	ErrInvalidTopologyType                                         = errors.New("InvalidTopologyType, code 294")
	ErrInvalidHeartBeatFrequency                                   = errors.New("InvalidHeartBeatFrequency, code 295")
	ErrTopologySetNameRequired                                     = errors.New("TopologySetNameRequired, code 296")
	ErrHierarchicalAcquisitionLevelViolation                       = errors.New("HierarchicalAcquisitionLevelViolation, code 297")
	ErrInvalidServerType                                           = errors.New("InvalidServerType, code 298")
	ErrOCSPCertificateStatusRevoked                                = errors.New("OCSPCertificateStatusRevoked, code 299")
	ErrRangeDeletionAbandonedBecauseCollectionWithUUIDDoesNotExist = errors.New("RangeDeletionAbandonedBecauseCollectionWithUUIDDoesNotExist, code 300")
	ErrDataCorruptionDetected                                      = errors.New("DataCorruptionDetected, code 301")
	ErrOCSPCertificateStatusUnknown                                = errors.New("OCSPCertificateStatusUnknown, code 302")
	ErrSplitHorizonChange                                          = errors.New("SplitHorizonChange, code 303")
	ErrShardInvalidatedForTargeting                                = errors.New("ShardInvalidatedForTargeting, code 304")
	ErrRangeDeletionAbandonedBecauseTaskDocumentDoesNotExist       = errors.New("RangeDeletionAbandonedBecauseTaskDocumentDoesNotExist, code 307")
	ErrCurrentConfigNotCommittedYet                                = errors.New("CurrentConfigNotCommittedYet, code 308")
	ErrExhaustCommandFinished                                      = errors.New("ExhaustCommandFinished, code 309")
	ErrPeriodicJobIsStopped                                        = errors.New("PeriodicJobIsStopped, code 310")
	ErrTransactionCoordinatorCanceled                              = errors.New("TransactionCoordinatorCanceled, code 311")
	ErrOperationIsKilledAndDelisted                                = errors.New("OperationIsKilledAndDelisted, code 312")
	ErrResumableRangeDeleterDisabled                               = errors.New("ResumableRangeDeleterDisabled, code 313")
	ErrObjectIsBusy                                                = errors.New("ObjectIsBusy, code 314")
	ErrTooStaleToSyncFromSource                                    = errors.New("TooStaleToSyncFromSource, code 315")
	ErrQueryTrialRunCompleted                                      = errors.New("QueryTrialRunCompleted, code 316")
	ErrConnectionPoolExpired                                       = errors.New("ConnectionPoolExpired, code 317")
	ErrForTestingOptionalErrorExtraInfo                            = errors.New("ForTestingOptionalErrorExtraInfo, code 318")
	ErrMovePrimaryInProgress                                       = errors.New("MovePrimaryInProgress, code 319")
	ErrTenantMigrationConflict                                     = errors.New("TenantMigrationConflict, code 320")
	ErrTenantMigrationCommitted                                    = errors.New("TenantMigrationCommitted, code 321")
	ErrAPIVersionError                                             = errors.New("APIVersionError, code 322")
	ErrAPIStrictError                                              = errors.New("APIStrictError, code 323")
	ErrAPIDeprecationError                                         = errors.New("APIDeprecationError, code 324")
	ErrTenantMigrationAborted                                      = errors.New("TenantMigrationAborted, code 325")
	ErrOplogQueryMinTsMissing                                      = errors.New("OplogQueryMinTsMissing, code 326")
	ErrNoSuchTenantMigration                                       = errors.New("NoSuchTenantMigration, code 327")
	ErrTenantMigrationAccessBlockerShuttingDown                    = errors.New("TenantMigrationAccessBlockerShuttingDown, code 328")
	ErrTenantMigrationInProgress                                   = errors.New("TenantMigrationInProgress, code 329")
	ErrSkipCommandExecution                                        = errors.New("SkipCommandExecution, code 330")
	ErrFailedToRunWithReplyBuilder                                 = errors.New("FailedToRunWithReplyBuilder, code 331")
	ErrCannotDowngrade                                             = errors.New("CannotDowngrade, code 332")
	ErrServiceExecutorInShutdown                                   = errors.New("ServiceExecutorInShutdown, code 333")
	ErrMechanismUnavailable                                        = errors.New("MechanismUnavailable, code 334")
	ErrTenantMigrationForgotten                                    = errors.New("TenantMigrationForgotten, code 335")
	ErrSocketException                                             = errors.New("SocketException, code 9001")
	ErrCannotGrowDocumentInCappedNamespace                         = errors.New("CannotGrowDocumentInCappedNamespace, code 10003")
	ErrNotWritablePrimary                                          = errors.New("NotWritablePrimary, code 10107")
	ErrBSONObjectTooLarge                                          = errors.New("BSONObjectTooLarge, code 10334")
	ErrDuplicateKey                                                = errors.New("DuplicateKey, code 11000")
	ErrInterruptedAtShutdown                                       = errors.New("InterruptedAtShutdown, code 11600")
	ErrInterrupted                                                 = errors.New("Interrupted, code 11601")
	ErrInterruptedDueToReplStateChange                             = errors.New("InterruptedDueToReplStateChange, code 11602")
	ErrBackgroundOperationInProgressForDatabase                    = errors.New("BackgroundOperationInProgressForDatabase, code 12586")
	ErrBackgroundOperationInProgressForNamespace                   = errors.New("BackgroundOperationInProgressForNamespace, code 12587")
	ErrMergeStageNoMatchingDocument                                = errors.New("MergeStageNoMatchingDocument, code 13113")
	ErrDatabaseDifferCase                                          = errors.New("DatabaseDifferCase, code 13297")
	ErrStaleConfig                                                 = errors.New("StaleConfig, code 13388")
	ErrNotPrimaryNoSecondaryOk                                     = errors.New("NotPrimaryNoSecondaryOk, code 13435")
	ErrNotPrimaryOrSecondary                                       = errors.New("NotPrimaryOrSecondary, code 13436")
	ErrOutOfDiskSpace                                              = errors.New("OutOfDiskSpace, code 14031")
	ErrClientMarkedKilled                                          = errors.New("ClientMarkedKilled, code 46841")
)

var errorMap = map[int32]error{
	1:     ErrInternalError,
	2:     ErrBadValue,
	4:     ErrNoSuchKey,
	5:     ErrGraphContainsCycle,
	6:     ErrHostUnreachable,
	7:     ErrHostNotFound,
	8:     ErrUnknownError,
	9:     ErrFailedToParse,
	10:    ErrCannotMutateObject,
	11:    ErrUserNotFound,
	12:    ErrUnsupportedFormat,
	13:    ErrUnauthorized,
	14:    ErrTypeMismatch,
	15:    ErrOverflow,
	16:    ErrInvalidLength,
	17:    ErrProtocolError,
	18:    ErrAuthenticationFailed,
	19:    ErrCannotReuseObject,
	20:    ErrIllegalOperation,
	21:    ErrEmptyArrayOperation,
	22:    ErrInvalidBSON,
	23:    ErrAlreadyInitialized,
	24:    ErrLockTimeout,
	25:    ErrRemoteValidationError,
	26:    ErrNamespaceNotFound,
	27:    ErrIndexNotFound,
	28:    ErrPathNotViable,
	29:    ErrNonExistentPath,
	30:    ErrInvalidPath,
	31:    ErrRoleNotFound,
	32:    ErrRolesNotRelated,
	33:    ErrPrivilegeNotFound,
	34:    ErrCannotBackfillArray,
	35:    ErrUserModificationFailed,
	36:    ErrRemoteChangeDetected,
	37:    ErrFileRenameFailed,
	38:    ErrFileNotOpen,
	39:    ErrFileStreamFailed,
	40:    ErrConflictingUpdateOperators,
	41:    ErrFileAlreadyOpen,
	42:    ErrLogWriteFailed,
	43:    ErrCursorNotFound,
	44:    ErrDuplicateKey,
	59:    ErrCommandNotFound,
	61:    ErrShardKeyNotFound,
	62:    ErrOplogOperationUnsupported,
	63:    ErrStaleShardVersion,
	64:    ErrWriteConcernFailed,
	65:    ErrMultipleErrorsOccurred,
	66:    ErrImmutableField,
	67:    ErrCannotCreateIndex,
	68:    ErrIndexAlreadyExists,
	69:    ErrAuthSchemaIncompatible,
	70:    ErrShardNotFound,
	71:    ErrReplicaSetNotFound,
	72:    ErrInvalidOptions,
	73:    ErrInvalidNamespace,
	74:    ErrNodeNotFound,
	75:    ErrWriteConcernLegacyOK,
	76:    ErrNoReplicationEnabled,
	77:    ErrOperationIncomplete,
	78:    ErrCommandResultSchemaViolation,
	79:    ErrUnknownReplWriteConcern,
	80:    ErrRoleDataInconsistent,
	81:    ErrNoMatchParseContext,
	82:    ErrNoProgressMade,
	83:    ErrRemoteResultsUnavailable,
	85:    ErrIndexOptionsConflict,
	86:    ErrIndexKeySpecsConflict,
	87:    ErrCannotSplit,
	89:    ErrNetworkTimeout,
	90:    ErrCallbackCanceled,
	91:    ErrShutdownInProgress,
	92:    ErrSecondaryAheadOfPrimary,
	93:    ErrInvalidReplicaSetConfig,
	94:    ErrNotYetInitialized,
	95:    ErrNotSecondary,
	96:    ErrOperationFailed,
	97:    ErrNoProjectionFound,
	98:    ErrDBPathInUse,
	100:   ErrUnsatisfiableWriteConcern,
	101:   ErrOutdatedClient,
	102:   ErrIncompatibleAuditMetadata,
	103:   ErrNewReplicaSetConfigurationIncompatible,
	104:   ErrNodeNotElectable,
	105:   ErrIncompatibleShardingMetadata,
	106:   ErrDistributedClockSkewed,
	107:   ErrLockFailed,
	108:   ErrInconsistentReplicaSetNames,
	109:   ErrConfigurationInProgress,
	110:   ErrCannotInitializeNodeWithData,
	111:   ErrNotExactValueField,
	112:   ErrWriteConflict,
	113:   ErrInitialSyncFailure,
	114:   ErrInitialSyncOplogSourceMissing,
	115:   ErrCommandNotSupported,
	116:   ErrDocTooLargeForCapped,
	117:   ErrConflictingOperationInProgress,
	118:   ErrNamespaceNotSharded,
	119:   ErrInvalidSyncSource,
	120:   ErrOplogStartMissing,
	121:   ErrDocumentValidationFailure,
	123:   ErrNotAReplicaSet,
	124:   ErrIncompatibleElectionProtocol,
	125:   ErrCommandFailed,
	126:   ErrRPCProtocolNegotiationFailed,
	127:   ErrUnrecoverableRollbackError,
	128:   ErrLockNotFound,
	129:   ErrLockStateChangeFailed,
	130:   ErrSymbolNotFound,
	133:   ErrFailedToSatisfyReadPreference,
	134:   ErrReadConcernMajorityNotAvailableYet,
	135:   ErrStaleTerm,
	136:   ErrCappedPositionLost,
	137:   ErrIncompatibleShardingConfigVersion,
	138:   ErrRemoteOplogStale,
	139:   ErrJSInterpreterFailure,
	140:   ErrInvalidSSLConfiguration,
	141:   ErrSSLHandshakeFailed,
	142:   ErrJSUncatchableError,
	143:   ErrCursorInUse,
	144:   ErrIncompatibleCatalogManager,
	145:   ErrPooledConnectionsDropped,
	146:   ErrExceededMemoryLimit,
	147:   ErrZLibError,
	148:   ErrReadConcernMajorityNotEnabled,
	149:   ErrNoConfigPrimary,
	150:   ErrStaleEpoch,
	151:   ErrOperationCannotBeBatched,
	152:   ErrOplogOutOfOrder,
	153:   ErrChunkTooBig,
	154:   ErrInconsistentShardIdentity,
	155:   ErrCannotApplyOplogWhilePrimary,
	157:   ErrCanRepairToDowngrade,
	158:   ErrMustUpgrade,
	159:   ErrDurationOverflow,
	160:   ErrMaxStalenessOutOfRange,
	161:   ErrIncompatibleCollationVersion,
	162:   ErrCollectionIsEmpty,
	163:   ErrZoneStillInUse,
	164:   ErrInitialSyncActive,
	165:   ErrViewDepthLimitExceeded,
	166:   ErrCommandNotSupportedOnView,
	167:   ErrOptionNotSupportedOnView,
	168:   ErrInvalidPipelineOperator,
	169:   ErrCommandOnShardedViewNotSupportedOnMongod,
	170:   ErrTooManyMatchingDocuments,
	171:   ErrCannotIndexParallelArrays,
	172:   ErrTransportSessionClosed,
	173:   ErrTransportSessionNotFound,
	174:   ErrTransportSessionUnknown,
	175:   ErrQueryPlanKilled,
	176:   ErrFileOpenFailed,
	177:   ErrZoneNotFound,
	178:   ErrRangeOverlapConflict,
	179:   ErrWindowsPdhError,
	180:   ErrBadPerfCounterPath,
	181:   ErrAmbiguousIndexKeyPattern,
	182:   ErrInvalidViewDefinition,
	183:   ErrClientMetadataMissingField,
	184:   ErrClientMetadataAppNameTooLarge,
	185:   ErrClientMetadataDocumentTooLarge,
	186:   ErrClientMetadataCannotBeMutated,
	187:   ErrLinearizableReadConcernError,
	188:   ErrIncompatibleServerVersion,
	189:   ErrPrimarySteppedDown,
	190:   ErrMasterSlaveConnectionFailure,
	192:   ErrFailPointEnabled,
	193:   ErrNoShardingEnabled,
	194:   ErrBalancerInterrupted,
	195:   ErrViewPipelineMaxSizeExceeded,
	197:   ErrInvalidIndexSpecificationOption,
	199:   ErrReplicaSetMonitorRemoved,
	200:   ErrChunkRangeCleanupPending,
	201:   ErrCannotBuildIndexKeys,
	202:   ErrNetworkInterfaceExceededTimeLimit,
	203:   ErrShardingStateNotInitialized,
	204:   ErrTimeProofMismatch,
	205:   ErrClusterTimeFailsRateLimiter,
	206:   ErrNoSuchSession,
	207:   ErrInvalidUUID,
	208:   ErrTooManyLocks,
	209:   ErrStaleClusterTime,
	210:   ErrCannotVerifyAndSignLogicalTime,
	211:   ErrKeyNotFound,
	212:   ErrIncompatibleRollbackAlgorithm,
	213:   ErrDuplicateSession,
	214:   ErrAuthenticationRestrictionUnmet,
	215:   ErrDatabaseDropPending,
	216:   ErrElectionInProgress,
	217:   ErrIncompleteTransactionHistory,
	218:   ErrUpdateOperationFailed,
	219:   ErrFTDCPathNotSet,
	220:   ErrFTDCPathAlreadySet,
	221:   ErrIndexModified,
	222:   ErrCloseChangeStream,
	223:   ErrIllegalOpMsgFlag,
	224:   ErrQueryFeatureNotAllowed,
	225:   ErrTransactionTooOld,
	226:   ErrAtomicityFailure,
	227:   ErrCannotImplicitlyCreateCollection,
	228:   ErrSessionTransferIncomplete,
	229:   ErrMustDowngrade,
	230:   ErrDNSHostNotFound,
	231:   ErrDNSProtocolError,
	232:   ErrMaxSubPipelineDepthExceeded,
	233:   ErrTooManyDocumentSequences,
	234:   ErrRetryChangeStream,
	235:   ErrInternalErrorNotSupported,
	236:   ErrForTestingErrorExtraInfo,
	237:   ErrCursorKilled,
	238:   ErrNotImplemented,
	239:   ErrSnapshotTooOld,
	240:   ErrDNSRecordTypeMismatch,
	241:   ErrConversionFailure,
	242:   ErrCannotCreateCollection,
	243:   ErrIncompatibleWithUpgradedServer,
	245:   ErrBrokenPromise,
	246:   ErrSnapshotUnavailable,
	247:   ErrProducerConsumerQueueBatchTooLarge,
	248:   ErrProducerConsumerQueueEndClosed,
	249:   ErrStaleDbVersion,
	250:   ErrStaleChunkHistory,
	251:   ErrNoSuchTransaction,
	252:   ErrReentrancyNotAllowed,
	253:   ErrFreeMonHttpInFlight,
	254:   ErrFreeMonHttpTemporaryFailure,
	255:   ErrFreeMonHttpPermanentFailure,
	256:   ErrTransactionCommitted,
	257:   ErrTransactionTooLarge,
	258:   ErrUnknownFeatureCompatibilityVersion,
	259:   ErrKeyedExecutorRetry,
	260:   ErrInvalidResumeToken,
	261:   ErrTooManyLogicalSessions,
	262:   ErrExceededTimeLimit,
	263:   ErrOperationNotSupportedInTransaction,
	264:   ErrTooManyFilesOpen,
	265:   ErrOrphanedRangeCleanUpFailed,
	266:   ErrFailPointSetFailed,
	267:   ErrPreparedTransactionInProgress,
	268:   ErrCannotBackup,
	269:   ErrDataModifiedByRepair,
	270:   ErrRepairedReplicaSetNode,
	271:   ErrJSInterpreterFailureWithStack,
	272:   ErrMigrationConflict,
	273:   ErrProducerConsumerQueueProducerQueueDepthExceeded,
	274:   ErrProducerConsumerQueueConsumed,
	275:   ErrExchangePassthrough,
	276:   ErrIndexBuildAborted,
	277:   ErrAlarmAlreadyFulfilled,
	278:   ErrUnsatisfiableCommitQuorum,
	279:   ErrClientDisconnect,
	280:   ErrChangeStreamFatalError,
	281:   ErrTransactionCoordinatorSteppingDown,
	282:   ErrTransactionCoordinatorReachedAbortDecision,
	283:   ErrWouldChangeOwningShard,
	284:   ErrForTestingErrorExtraInfoWithExtraInfoInNamespace,
	285:   ErrIndexBuildAlreadyInProgress,
	286:   ErrChangeStreamHistoryLost,
	287:   ErrTransactionCoordinatorDeadlineTaskCanceled,
	288:   ErrChecksumMismatch,
	289:   ErrWaitForMajorityServiceEarlierOpTimeAvailable,
	290:   ErrTransactionExceededLifetimeLimitSeconds,
	291:   ErrNoQueryExecutionPlans,
	292:   ErrQueryExceededMemoryLimitNoDiskUseAllowed,
	293:   ErrInvalidSeedList,
	294:   ErrInvalidTopologyType,
	295:   ErrInvalidHeartBeatFrequency,
	296:   ErrTopologySetNameRequired,
	297:   ErrHierarchicalAcquisitionLevelViolation,
	298:   ErrInvalidServerType,
	299:   ErrOCSPCertificateStatusRevoked,
	300:   ErrRangeDeletionAbandonedBecauseCollectionWithUUIDDoesNotExist,
	301:   ErrDataCorruptionDetected,
	302:   ErrOCSPCertificateStatusUnknown,
	303:   ErrSplitHorizonChange,
	304:   ErrShardInvalidatedForTargeting,
	307:   ErrRangeDeletionAbandonedBecauseTaskDocumentDoesNotExist,
	308:   ErrCurrentConfigNotCommittedYet,
	309:   ErrExhaustCommandFinished,
	310:   ErrPeriodicJobIsStopped,
	311:   ErrTransactionCoordinatorCanceled,
	312:   ErrOperationIsKilledAndDelisted,
	313:   ErrResumableRangeDeleterDisabled,
	314:   ErrObjectIsBusy,
	315:   ErrTooStaleToSyncFromSource,
	316:   ErrQueryTrialRunCompleted,
	317:   ErrConnectionPoolExpired,
	318:   ErrForTestingOptionalErrorExtraInfo,
	319:   ErrMovePrimaryInProgress,
	320:   ErrTenantMigrationConflict,
	321:   ErrTenantMigrationCommitted,
	322:   ErrAPIVersionError,
	323:   ErrAPIStrictError,
	324:   ErrAPIDeprecationError,
	325:   ErrTenantMigrationAborted,
	326:   ErrOplogQueryMinTsMissing,
	327:   ErrNoSuchTenantMigration,
	328:   ErrTenantMigrationAccessBlockerShuttingDown,
	329:   ErrTenantMigrationInProgress,
	330:   ErrSkipCommandExecution,
	331:   ErrFailedToRunWithReplyBuilder,
	332:   ErrCannotDowngrade,
	333:   ErrServiceExecutorInShutdown,
	334:   ErrMechanismUnavailable,
	335:   ErrTenantMigrationForgotten,
	9001:  ErrSocketException,
	10003: ErrCannotGrowDocumentInCappedNamespace,
	10107: ErrNotWritablePrimary,
	10334: ErrBSONObjectTooLarge,
	11000: ErrDuplicateKey,
	11600: ErrInterruptedAtShutdown,
	11601: ErrInterrupted,
	11602: ErrInterruptedDueToReplStateChange,
	12586: ErrBackgroundOperationInProgressForDatabase,
	12587: ErrBackgroundOperationInProgressForNamespace,
	13113: ErrMergeStageNoMatchingDocument,
	13297: ErrDatabaseDifferCase,
	13388: ErrStaleConfig,
	13435: ErrNotPrimaryNoSecondaryOk,
	13436: ErrNotPrimaryOrSecondary,
	14031: ErrOutOfDiskSpace,
	46841: ErrClientMarkedKilled,
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
