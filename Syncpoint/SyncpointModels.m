//
//  SyncpointModels.m
//  Syncpoint
//
//  Created by Jens Alfke on 3/7/12.
//  Copyright (c) 2012 Couchbase, Inc. All rights reserved.
//

#import "SyncpointModels.h"
#import "SyncpointInternal.h"
#import "CouchModelFactory.h"
#import "TDMisc.h"
#import "CollectionUtils.h"
#import <Security/SecRandom.h>


@interface CouchModel (Internal)
- (CouchModel*) getModelProperty: (NSString*)property;
- (void) setModel: (CouchModel*)model forProperty: (NSString*)property;
@end


static NSString* randomString(void) {
    uint8_t randomBytes[16];    // 128 bits of entropy
    SecRandomCopyBytes(kSecRandomDefault, sizeof(randomBytes), randomBytes);
    return TDHexFromBytes(randomBytes, sizeof(randomBytes));
}


//TODO: This would be useful as a method in CouchModelFactory or CouchDatabase...
static NSEnumerator* modelsOfType(CouchDatabase* database, NSString* type) {
    NSEnumerator* e = [[database getAllDocuments] rows];
//    LogTo(Syncpoint, @"modelsOfType %@ for database with %u docs", type, [[[database getAllDocuments] rows] count]);
    return [e my_map: ^(CouchQueryRow* row) {
        if ([type isEqual: [row.documentProperties objectForKey: @"type"]]) {
            CouchModel* model = [CouchModel modelForDocument: row.document];
            if (!model) {
                LogTo(Syncpoint, @"did you register a class for %@?",type);
            }
            return model;
        }
        else
            return nil;
    }];
}




@implementation SyncpointModel

@dynamic state;

- (bool) isActive {
    return [self.state isEqual: @"active"];
}

// FIX: This name-mapping should be moved into CouchModel itself somehow.
- (CouchModel*) getModelProperty: (NSString*)property {
    return [super getModelProperty: [property stringByAppendingString: @"_id"]];
}

- (void) setModel: (CouchModel*)model forProperty: (NSString*)property {
    [super setModel: model forProperty: [property stringByAppendingString: @"_id"]];
}

+ (Class) classOfProperty: (NSString*)property {
    if ([property hasSuffix: @"_id"])
        property = [property substringToIndex: property.length-3];
    return [super classOfProperty: property];
}

@end




@implementation SyncpointSession

@dynamic owner_id, oauth_creds, pairing_creds, channel_database, control_database, control_db_synced;

- (bool) isPaired {
    return [self.state isEqual: @"paired"];
}

- (bool) isReadyToPair {
    return !![self getValueOfProperty:@"pairing_token"];
}

+ (SyncpointSession*) sessionInDatabase: (CouchDatabase *)database {
    NSString* sessID = [[NSUserDefaults standardUserDefaults] objectForKey:@"Syncpoint_SessionDocID"];
    if (!sessID)
        return nil;
    CouchDocument* doc = [database documentWithID: sessID];
    if (!doc)
        return nil;
    if (!doc.properties) {
        // Oops -- the session ID in user-defaults is out of date, so clear it
        [[NSUserDefaults standardUserDefaults] removeObjectForKey: @"Syncpoint_SessionDocID"];
        return nil;
    }
    return [self modelForDocument: doc];
}


+ (SyncpointSession*) makeSessionInDatabase: (CouchDatabase*)database
                                      appId: (NSString*)appId
                               multiChannel: (BOOL) multi
                           withRemoteServer: (NSURL*) remote
                                      error: (NSError**)outError
{
    // Register the other model classes with the database's model factory:
    CouchModelFactory* factory = database.modelFactory;
    [factory registerClass: @"SyncpointChannel" forDocumentType: @"channel"];
    [factory registerClass: @"SyncpointSubscription" forDocumentType: @"subscription"];
    [factory registerClass: @"SyncpointInstallation" forDocumentType: @"installation"];

    
    LogTo(Syncpoint, @"Creating session for %@ in %@", appId, database);
    
    
    SyncpointSession* session = [[self alloc] initWithNewDocumentInDatabase: database];
    [session setValue: appId ofProperty: @"app_id"];
    [session setValue: [remote absoluteString] ofProperty: @"syncpoint_url"];
    session.state = @"new";
    
    NSDictionary* oauth_creds = $dict({@"consumer_key", randomString()},
                                      {@"consumer_secret", randomString()},
                                      {@"token_secret", randomString()},
                                      {@"token", randomString()});
    session.oauth_creds = oauth_creds;

    NSDictionary* pairingCreds = $dict({@"username", [@"pairing-" stringByAppendingString:randomString()]},
                                  {@"password", randomString()});
    session.pairing_creds = pairingCreds;
    
    if (multi) {
        [session setValue: @"multi-channel" ofProperty: @"pairing_mode"];
    } else {
        [session setValue: @"single-channel" ofProperty: @"pairing_mode"];
    }
    
    if (![[session save] wait: outError]) {
        Warn(@"SyncpointSession: Couldn't save new session");
        return nil;
    }
    
    NSString* sessionID = session.document.documentID;
    LogTo(Syncpoint, @"...session ID = %@", sessionID);
    NSUserDefaults *defaults = [NSUserDefaults standardUserDefaults];
    [defaults setObject: sessionID forKey: @"Syncpoint_SessionDocID"];
    [defaults synchronize];
    return session;
}


- (id) initWithDocument: (CouchDocument*)document {
    self = [super initWithDocument: document];
    if (self) {
        // Register the other model classes with the database's model factory:
        CouchModelFactory* factory = self.database.modelFactory;
        [factory registerClass: @"SyncpointChannel" forDocumentType: @"channel"];
        [factory registerClass: @"SyncpointSubscription" forDocumentType: @"subscription"];
        [factory registerClass: @"SyncpointInstallation" forDocumentType: @"installation"];
    }
    return self;
}

- (NSDictionary*) pairingUserProperties {
    NSString* username = [self.pairing_creds objectForKey:@"username"];
    NSString* password = [self.pairing_creds objectForKey:@"password"];
    NSAssert(username, @"needs the pairing username set first");
    NSAssert(password, @"needs the pairing password set first");
    
    return $dict({@"_id", $sprintf(@"org.couchdb.user:%@", username)},
                 {@"name", username},
                 {@"type", @"user"},
                 {@"sp_oauth",self.oauth_creds},
                 {@"pairing_state", @"new"},
                 {@"pairing_type",[self getValueOfProperty:@"pairing_type"]},
                 {@"pairing_mode",[self getValueOfProperty:@"pairing_mode"]},
                 {@"pairing_token",[self getValueOfProperty:@"pairing_token"]},
                 {@"pairing_app_id",[self getValueOfProperty:@"app_id"]},
                 {@"roles", [NSArray array]},
                 {@"password", password});
}

- (NSError*) error {
    if (![self.state isEqual: @"error"])
        return nil;
    NSDictionary* errDict = [self getValueOfProperty: @"error"];
    int code = [$castIf(NSNumber, [errDict objectForKey: @"errno"]) intValue];
    NSString* message = $castIf(NSString, [errDict objectForKey: @"message"]);
    return [NSError errorWithDomain: NSPOSIXErrorDomain
                               code: (code ? code : -1)     // don't allow a zero code
                           userInfo: $dict({NSLocalizedDescriptionKey, message})];
}


- (BOOL) clearState: (NSError**)outError {
    self.state = @"new";
    [self setValue: nil ofProperty: @"error"];
    return [[self save] wait: outError];
}


- (SyncpointChannel*) makeChannelWithName: (NSString*)name
                                    error: (NSError**)outError
{
    LogTo(Syncpoint, @"Create channel named '%@'", name);
    SyncpointChannel* channel = [[SyncpointChannel alloc] initWithNewDocumentInDatabase: self.database];
    [channel setValue: @"channel" ofProperty: @"type"];
    
    if (self.owner_id) {
//        [channel setValue: self.owner_id ofProperty: @"owner_id"];
        channel.owner_id = self.owner_id;
        channel.state = @"new";
    } else {
        channel.owner_id = @"unpaired";
        channel.state = @"unpaired";
    }

    channel.name = name;
    return [[channel save] wait: outError] ? channel : nil;
}


- (SyncpointChannel*) channelWithName: (NSString*)name andOwner: (NSString*)ownerId{
    // TODO: Make this into a view query
    LogTo(Syncpoint, @"Looking for channel named %@ with owner_id %@", name, ownerId);

    for (SyncpointChannel* channel in modelsOfType(self.database, @"channel")) {
        LogTo(Syncpoint, @"Saw channel named %@ with owner_id %@ and state %@", channel.name, channel.owner_id, channel.state);
        if (![channel.state isEqual: @"error"] && [channel.name isEqual: name] && [channel.owner_id isEqual:ownerId])
            return channel;
    }
    LogTo(Syncpoint, @"channelWithName %@ returning nil ", name);

    return nil;
}

- (SyncpointChannel*) myChannelWithName: (NSString*)name {
    id owner;
    if (self.owner_id) {
        owner = self.owner_id;
    } else {
        owner = @"unpaired";
    }
    
    return [self channelWithName:name andOwner:owner];
}


- (void) didFirstSyncOfControlDB {
    if(!self.control_db_synced) {
        self.control_db_synced = YES;
        [[self save] wait: nil];
    }
}


- (NSEnumerator*) readyChannels {
    // TODO: Make this into a view query
    return [modelsOfType(self.database, @"channel") my_map: ^(SyncpointChannel* channel) {
        return channel.isReady ? channel : nil;
    }];
}

- (NSEnumerator*) unpairedChannels {
    // TODO: Make this into a view query
    return [modelsOfType(self.database, @"channel") my_map: ^(SyncpointChannel* channel) {
        return channel.unpaired ? channel : nil;
    }];
}

- (NSEnumerator*) myChannels {
    // TODO: Make this into a view query
    return [modelsOfType(self.database, @"channel") my_map: ^(SyncpointChannel* channel) {
        return ([channel.owner_id isEqual: self.owner_id]) ? channel : nil;
    }];
}

- (NSEnumerator*) activeSubscriptions {
    // TODO: Make this into a view query
    // TODO: ensure the subscription.owner_id matches the session.owner_id
    return [modelsOfType(self.database, @"subscription") my_map: ^(SyncpointSubscription* sub) {
        return sub.isActive ? sub : nil;
    }];
}


- (NSSet*) installedSubscriptions {
    NSMutableSet* subscriptions = [NSMutableSet set];
    for (SyncpointInstallation* inst in self.allInstallations)
        [subscriptions addObject: inst.subscription];
    return subscriptions;
}


- (NSEnumerator*) allInstallations {
    // TODO: Make this into a view query
    return [modelsOfType(self.database, @"installation") my_map: ^(SyncpointInstallation* inst) {
        return ([inst.state isEqual: @"created"] && inst.session == self) ? inst : nil;
    }];
}


@end




@implementation SyncpointChannel

@dynamic name, owner_id, cloud_database;

- (bool) isReady {
    return [self.state isEqual: @"ready"];
}

- (bool) unpaired {
    return [self.state isEqual: @"unpaired"];
}

- (SyncpointSubscription*) subscription {
    // TODO: Make this into a view query
    for (SyncpointSubscription* sub in modelsOfType(self.database, @"subscription"))
        if (sub.channel == self)
            return sub;
    return nil;
}

- (CouchDatabase*) localDatabase {
    SyncpointInstallation* inst = [self installation];
    if (inst) {
        return [inst localDatabase];
    } else {
        return nil;
    }
}

- (CouchDatabase*) ensureLocalDatabase: (NSError**)outError
 {
     SyncpointSubscription *sub = [self subscription];
     if (!sub) {
         sub = [self subscribe: outError];
     };
     if (!sub) {
         return nil;
     }
     if (![self localDatabase]) {
         [sub makeInstallationWithLocalDatabase:nil error: outError];
     }
     CouchDatabase *database = [self localDatabase];
     if (!database) {
         return nil;
     }
     return database;
}


- (SyncpointInstallation*) installation {
    // TODO: Make this into a view query
    for (SyncpointInstallation* inst in modelsOfType(self.database, @"installation"))
        if (inst.channel == self && inst.isLocal)
            return inst;
    return nil;
}


- (SyncpointSubscription*) subscribe: (NSError**)outError {
    LogTo(Syncpoint, @"Subscribing to %@", self);
    SyncpointSubscription* sub = [[SyncpointSubscription alloc] initWithNewDocumentInDatabase: self.database];
    [sub setValue: @"subscription" ofProperty: @"type"];
    sub.state = @"active";
    [sub setValue: [self getValueOfProperty: @"owner_id"] ofProperty: @"owner_id"];
    sub.channel = self;
    return [[sub save] wait: outError] ? sub : nil;
}

@end




@implementation SyncpointSubscription

@dynamic channel, owner_id;


- (SyncpointInstallation*) installation {
    return self.channel.installation;
}


- (SyncpointInstallation*) makeInstallationWithLocalDatabase: (CouchDatabase*)localDB
                                                       error: (NSError**)outError
{
    NSString* name;
    if (localDB)
        name = localDB.relativePath;
    else { 
        name = [@"channel-" stringByAppendingString: randomString()];
        localDB = [self.database.server databaseNamed: name];
    }

    LogTo(Syncpoint, @"Installing %@ to %@", self, localDB);
    if (![localDB ensureCreated: nil]) {
        Warn(@"SyncpointSubscription could not create channel db %@", name);
        return nil;
    }

    SyncpointInstallation* inst = [[SyncpointInstallation alloc] initWithNewDocumentInDatabase: self.database];
    [inst setValue: @"installation" ofProperty: @"type"];
    inst.state = @"created";
    inst.session = [SyncpointSession sessionInDatabase: self.database];
    inst.owner_id = self.owner_id;
//    [inst setValue: [self getValueOfProperty: @"owner_id"] ofProperty: @"owner_id"];
    inst.channel = self.channel;
    inst.subscription = self;
    [inst setValue: name ofProperty: @"local_db_name"];
    return [[inst save] wait: outError] ? inst : nil;
}


- (BOOL) unsubscribe: (NSError**)outError {
    SyncpointInstallation* inst = self.installation;
    if (inst && ![inst uninstall: outError])
        return NO;
    return [[self deleteDocument] wait: outError];
    //????: Is this how to do it?
}


@end




@implementation SyncpointInstallation

@dynamic subscription, channel, session, owner_id;

- (CouchDatabase*) localDatabase {
    if (!self.isLocal)
        return nil;
    NSString* name = $castIf(NSString, [self getValueOfProperty: @"local_db_name"]);
    return name ? [self.database.server databaseNamed: name] : nil;
}

- (bool) isLocal {
    SyncpointSession* session = [SyncpointSession sessionInDatabase: self.database];
    return [session.document.documentID isEqual: [self getValueOfProperty: @"session_id"]];
}

// Starts bidirectional sync of an application database with its server counterpart.
- (void) sync {
    CouchDatabase *localChannelDb = self.localDatabase;
    NSURL *cloudChannelURL = [NSURL URLWithString: self.channel.cloud_database
                                    relativeToURL: [NSURL URLWithString:[self.session getValueOfProperty:@"syncpoint_url"]]];
    LogTo(Syncpoint, @"Sync local db '%@' with remote %@", localChannelDb, cloudChannelURL);
    NSArray* repls = [localChannelDb replicateWithURL: cloudChannelURL exclusively: NO];
    for (CouchPersistentReplication* repl in repls)
        repl.continuous = YES;
}

- (BOOL) uninstall: (NSError**)outError {
//    todo delete the database file here
    return [[self deleteDocument] wait: outError];
}

@end
