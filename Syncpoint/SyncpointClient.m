//
//  SyncpointClient.m
//  Syncpoint
//
//  Created by Jens Alfke on 2/23/12.
//  Copyright (c) 2012 Couchbase, Inc. All rights reserved.
//

#import "SyncpointClient.h"
#import "SyncpointModels.h"
#import "SyncpointInternal.h"
#import "CouchCocoa.h"
#import "TDMisc.h"
#import "MYBlockUtils.h"


#define kLocalControlDatabaseName @"sp_control"
#define kLocalChannelDatabaseName @"sp_channel"


@interface SyncpointClient ()
@end


@implementation SyncpointClient
{
    @private
    NSURL* _remote;
    NSString* _appId;
    CouchServer* _server;
    CouchDatabase* _localControlOrChannelDatabase;
    SyncpointSession* _session;
    CouchReplication *_syncpointPull;
    CouchReplication *_syncpointPush;
    BOOL _observingControlPull;
    BOOL _singleChannelMode;
}


@synthesize localServer=_server, session=_session, appId=_appId;

- (id) initWithRemoteServer: (NSURL*)remoteServerURL
                     appId: (NSString*)syncpointAppId
               multiChannel: (BOOL) multi
                     error: (NSError**)outError
{
    CouchTouchDBServer* newLocalServer = [CouchTouchDBServer sharedInstance];
    return [self initWithLocalServer:newLocalServer remoteServer:remoteServerURL appId:syncpointAppId multiChannel:multi error:outError];
}

- (id) initWithLocalServer: (CouchServer*)localServer
              remoteServer: (NSURL*)remoteServerURL
                     appId: (NSString*)syncpointAppId
              multiChannel: (BOOL) multi
                     error: (NSError**)outError
{
    CAssert(localServer);
    CAssert(remoteServerURL);
    self = [super init];
    if (self) {
        _server = localServer;
        _remote = remoteServerURL;
        _appId = syncpointAppId;
        
        if (multi) {
            // Create the control database on the first run of the app.
            _localControlOrChannelDatabase = [self setupControlDatabaseNamed: kLocalControlDatabaseName error: outError];            
        } else {
            // Create the sync channel database on the first run of the app.
            _localControlOrChannelDatabase = [_server databaseNamed: kLocalChannelDatabaseName];
            if (![_localControlOrChannelDatabase ensureCreated: outError])
                return nil;
        }
        if (!_localControlOrChannelDatabase) return nil;

        _session = [SyncpointSession sessionInDatabase: _localControlOrChannelDatabase];
        if (!_session) { // if no session make one
            _session = [SyncpointSession makeSessionInDatabase: _localControlOrChannelDatabase
                                                         appId: _appId
                                                  multiChannel: multi
                                              withRemoteServer: _remote
                                                         error: nil];   // TODO: Report error
        }
        if (nil != _session.error) {
            LogTo(Syncpoint, @"Session has error: %@", _session.error.localizedDescription);
        }
        if (_session.isPaired) {
            LogTo(Syncpoint, @"Session is active");
            [self connectToControlOrChannelDB];
        } else if (_session.isReadyToPair) {
            LogTo(Syncpoint, @"Begin pairing with cloud: %@", _remote.absoluteString);
            [self beginPairing];
        }
    }
    return self;
}

- (void)dealloc {
    [self stopObservingControlPull];
    [[NSNotificationCenter defaultCenter] removeObserver: self];
}

- (SyncpointChannel*) myChannelNamed: (NSString*)channelName 
                               error: (NSError**)error {
    SyncpointChannel* channel = [_session myChannelWithName:channelName];
    if (!channel) {
        channel = [_session makeChannelWithName: channelName error:error];
        if (!channel) return nil;
    }
    return channel;
}

- (CouchDatabase*) myDatabase {
    _singleChannelMode = YES;
    return _localControlOrChannelDatabase;
}


#pragma mark - Views and Queries

- (CouchLiveQuery*) myChannelsQuery {
    CouchLiveQuery* query = [[[_localControlOrChannelDatabase designDocumentWithName: @"syncpoint"]
                              queryViewNamed: @"channels"] asLiveQuery];
    id owner;
    if (_session.owner_id) {
        owner = _session.owner_id;
    } else {
        owner = @"unpaired";
    }
    query.descending = YES;
    query.keys = $array(owner);
    return query;
}

- (CouchDatabase*) setupControlDatabaseNamed: (NSString*)name error: (NSError**)outError {
    CouchDatabase* database = [_server databaseNamed: name];
    if (![database ensureCreated: outError])
        return nil;
    
    // Create a 'view' of known channels by owner:
    CouchDesignDocument* design = [database designDocumentWithName: @"syncpoint"];
    [design defineViewNamed: @"channels" mapBlock: MAPBLOCK({
        NSString* type = $castIf(NSString, [doc objectForKey: @"type"]);
        if ([type isEqualToString:@"channel"]) {
            emit([doc objectForKey: @"owner_id"], doc);
        }
    }) version: @"1.1"];
    
    database.tracksChanges = YES;
    return database;
}


#pragma mark - Pairing with cloud

- (void) pairSessionWithType: (NSString*)pairingType andToken: (NSString*)pairingToken {
    if (_session.isPaired) return;
    [_session setValue: pairingType ofProperty: @"pairing_type"];
    [_session setValue: pairingToken ofProperty: @"pairing_token"];
    [[_session save] wait: nil];
    [self beginPairing];
}

- (void) beginPairing {
    LogTo(Syncpoint, @"Pairing session...");
    if (_session.isReadyToPair) {
        Assert(!_session.isPaired);
        [_session clearState: nil];
        [self savePairingUserToRemote];
    }
}

- (void) savePairingUserToRemote {
    CouchServer* anonRemote = [[CouchServer alloc] initWithURL: _remote];
    RESTResource* remoteSession = [[RESTResource alloc] initWithParent: anonRemote relativePath: @"_session"];
    RESTOperation* op = [remoteSession GET];
    [op onCompletion: ^{
        NSDictionary* resp = $castIf(NSDictionary, op.responseBody.fromJSON);
        NSString* userDbName = [[resp objectForKey:@"info"] objectForKey:@"authentication_db"];
        CouchDatabase* anonUserDb = [anonRemote databaseNamed:userDbName];
        NSDictionary* userProps = [_session pairingUserProperties];
        CouchDocument* newUserDoc = [anonUserDb documentWithID:[userProps objectForKey:@"_id"]];
        RESTOperation* docPut = [newUserDoc putProperties:userProps];
        [docPut onCompletion:^{
            NSString* remoteURLString = [[_remote absoluteString] 
                                         stringByReplacingOccurrencesOfString:@"://" 
                                         withString:$sprintf(@"://%@:%@@", 
                                                             [_session.pairing_creds objectForKey:@"username"], 
                                                             [_session.pairing_creds objectForKey:@"password"])];
            CouchServer* userRemote = [[CouchServer alloc] initWithURL: [NSURL URLWithString:remoteURLString]];
            CouchDatabase* userUserDb = [userRemote databaseNamed:userDbName];
            CouchDocument* readUserDoc = [userUserDb documentWithID: [newUserDoc documentID]];
            [self waitForPairingToComplete: readUserDoc];
        }];
    }];
    [op start];
}

- (void) waitForPairingToComplete: (CouchDocument*)userDoc {
    MYAfterDelay(3.0, ^{
        RESTOperation* op = [userDoc GET];
        [op onCompletion:^{
            NSDictionary* resp = $castIf(NSDictionary, op.responseBody.fromJSON);
            NSString* state = [resp objectForKey:@"pairing_state"];
            if ([state isEqualToString:@"paired"]) {
                [self pairingDidComplete: userDoc];
            } else {
                [self waitForPairingToComplete: userDoc];                
            }
        }];
        [op start];
    });
}

- (void) pairingDidComplete: (CouchDocument*)userDoc {
    NSMutableDictionary* props = [[userDoc properties] mutableCopy];

    [_session setValue:@"paired" forKey:@"state"];
    [_session setValue:[props valueForKey:@"owner_id"] ofProperty:@"owner_id"];
    [_session setValue:[props valueForKey:@"control_database"] ofProperty:@"control_database"];
    [_session setValue:[props valueForKey:@"channel_database"] ofProperty:@"channel_database"];
    RESTOperation* op = [_session save];
    [op onCompletion:^{
        LogTo(Syncpoint, @"Device is now paired");
        [props setObject:[NSNumber numberWithBool:YES] forKey:@"_deleted"];
        [[userDoc currentRevision] putProperties: props];
        [self connectToControlOrChannelDB];
    }];
}


#pragma mark - Connect to Control Database

- (CouchReplication*) pullFromSyncpointDatabaseNamed: (NSString*)dbName {
    NSURL* url = [NSURL URLWithString: dbName relativeToURL: _remote];
    return [_localControlOrChannelDatabase pullFromDatabaseAtURL: url];
}

- (CouchReplication*) pushToSyncpointDatabaseNamed: (NSString*)dbName {
    NSURL* url = [NSURL URLWithString: dbName relativeToURL: _remote];
    return [_localControlOrChannelDatabase pushToDatabaseAtURL: url];
}

// Start bidirectional sync with the control or channel database.
- (void) connectToControlOrChannelDB {
    if (_session.channel_database) {
        LogTo(Syncpoint, @"connectToChannelDB %@", (_session.channel_database));
        _syncpointPull = [self pullFromSyncpointDatabaseNamed: _session.channel_database];
        _syncpointPull.continuous = YES; 
        _syncpointPush = [self pushToSyncpointDatabaseNamed: _session.channel_database];
        _syncpointPush.continuous = YES;
    } else {
        LogTo(Syncpoint, @"connectToControlDB %@", (_session.control_database));
        if (!_session.control_db_synced) {
            [self doInitialSyncOfControlDB]; // sync once before we write
        } else {
            [self didInitialSyncOfControlDB]; // go continuous
        }
    }
}

- (void) doInitialSyncOfControlDB {
    if (!_observingControlPull) {
        // During the initial sync, make the pull non-continuous, and observe when it stops.
        // That way we know when the control DB has been fully updated from the server.
        // Once it has stopped, we can fire the didSyncControlDB event on the session,
        // and restart the sync in continuous mode.
        LogTo(Syncpoint, @"doInitialSyncOfControlDB");
        _syncpointPull = [self pullFromSyncpointDatabaseNamed: _session.control_database];
        [_syncpointPull addObserver: self forKeyPath: @"running" options: 0 context: NULL];
        _observingControlPull = YES;
    }
}

- (void) didInitialSyncOfControlDB {
    LogTo(Syncpoint, @"didInitialSyncOfControlDB");
    // Now we can sync continuously & push
    _syncpointPull = [self pullFromSyncpointDatabaseNamed: _session.control_database];
    _syncpointPull.continuous = YES; 
    _syncpointPush = [self pushToSyncpointDatabaseNamed: _session.control_database];
    _syncpointPush.continuous = YES;
    [_session didFirstSyncOfControlDB];
    MYAfterDelay(1.0, ^{
        [self getUpToDateWithSubscriptions];
        [self observeControlDatabase];
    });
}

// Observes when the initial _syncpointPull stops running, after -doInitialSyncOfControlDB.
- (void) observeValueForKeyPath: (NSString*)keyPath ofObject: (id)object 
                         change: (NSDictionary*)change context: (void*)context
{
    if (object == _syncpointPull && !_syncpointPull.running) {
//        first Control database sync is done
        [self stopObservingControlPull];
        [self mergeExistingChannels];
        [self didInitialSyncOfControlDB];
    }
}


- (void) stopObservingControlPull {
    if (_observingControlPull) {
        [_syncpointPull removeObserver: self forKeyPath: @"running"];
        _observingControlPull = NO;
    }
}



#pragma mark - React to Control Database Changes


// Begins observing document changes in the _localControlOrChannelDatabase.
- (void) observeControlDatabase {
    Assert(_localControlOrChannelDatabase);
    [[NSNotificationCenter defaultCenter] addObserver: self 
                                             selector: @selector(controlDatabaseChanged)
                                                 name: kCouchDatabaseChangeNotification 
                                               object: _localControlOrChannelDatabase];
}

- (void) controlDatabaseChanged {
    // if we are done with first ever sync
    if (_session.control_db_synced) {
        LogTo(Syncpoint, @"Control DB changed");
        // collect 1 second of changes before acting
//        todo can we make these calls all collapse into one?
        MYAfterDelay(1.0, ^{
            [self getUpToDateWithSubscriptions];
        });
    }
}


// Called when the control database changes or is initial pulled from the server.
- (void) getUpToDateWithSubscriptions {
    if (_singleChannelMode) {
        return;
    }
    LogTo(Syncpoint, @"getUpToDateWithSubscriptions");
    // Make installations for any subscriptions that don't have one:
    NSSet* installedSubscriptions = _session.installedSubscriptions;
    for (SyncpointSubscription* sub in _session.activeSubscriptions) {
        if (![installedSubscriptions containsObject: sub]) {
            LogTo(Syncpoint, @"Making installation db for %@", sub);
            [sub makeInstallationWithLocalDatabase: nil error: nil];    // TODO: Report error
        }
    }
    // Sync all installations whose channels are ready:
    for (SyncpointInstallation* inst in _session.allInstallations)
        if (inst.channel.isReady)
            [inst sync];
}

#pragma mark - Merge Session Data on Initial Pairing Sync


- (void) mergeExistingChannels {
    LogTo(Syncpoint, @"mergeExistingChannels");
    NSEnumerator* pairedChannels = _session.myChannels;
    BOOL matched;
    for (SyncpointChannel* unpaired in _session.unpairedChannels) {
        matched = NO;
        for (SyncpointChannel* paired in pairedChannels) {
            if ([paired.name isEqual:unpaired.name]) {
                matched = YES;
                [self mergeUnpairedChannel: unpaired intoPairedChannel: paired];
            }
        }
        if (!matched) {
            unpaired.state = @"new";
            unpaired.owner_id = _session.owner_id;
            [[unpaired save] wait];
        }
    }
}

//        if remote subscription exists
//            replace local subscription with remote subscription (update local install if exists)
//            else
//                update local subscription with cloud channel id
//                update local installation channel_id of cloud channel
- (void) mergeUnpairedChannel: (SyncpointChannel*) unpaired 
            intoPairedChannel: (SyncpointChannel*) paired {
    SyncpointSubscription* unpairedSub = unpaired.subscription;
    SyncpointSubscription* pairedSub = paired.subscription;
    SyncpointInstallation* unpairedInst = unpaired.installation;
    if (pairedSub) {
        if (unpairedInst) {
            unpairedInst.subscription = paired.subscription;
        }
        [[unpairedSub deleteDocument] wait];
    } else {
        unpairedSub.channel = paired;
        unpairedSub.owner_id = paired.owner_id;
        [[unpairedSub save] wait];
    }
    if (unpairedInst) {
        unpairedInst.owner_id = paired.owner_id;
        unpairedInst.channel = paired;
        [[unpairedInst save] wait];
    }
    [[unpaired deleteDocument] wait];
}



@end
