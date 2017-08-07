const request = require('request');
const async   = require('async');
const fs      = require('fs');
const util    = require('util');
const jsonfile= require('jsonfile');
const utility = require('./lib/utility');
const SpotifyWebApi = require('spotify-web-api-node');
const zlib = require("zlib");
const levelup = require('levelup');
const requestretry = require('requestretry');

// ===== Global ===== 
const CONSTANTS = {
  // Argument
  SECTION_SPLITTER:   ",", // don't use special character that a file name can't contain such as \/:*?"<>|
  SECTION_DEFAULT:    "all,gender,likes,music",

  SECTION_ALL:        "all",
  SECTION_GENDER:     "gender",
  SECTION_LIKES:      "likes",
  SECTION_MUSIC:      "music",

  TYPE_EVENT:         "event",
  TYPE_BRAND:         "brand",

  // Source APIs
  srcURL:             "https://www.theticketfairy.com/api/customer-ids/", // customer ids
  spotifyTrackPath:   "https://open.spotify.com/track/",
  spotifyAlbumPath:   "https://open.spotify.com/album/",
  spotifyTrackAPI:    "https://api.spotify.com/v1/tracks/",
  spotifyArtistAPI:   "https://api.spotify.com/v1/artists/",
  spotifyClientID:    "dd0da1129f9943808fadea48a66c575a",
  spotifyClientSecret:"b099b35f8329406b95a19276b57d410c",

  // Destination APIs
  baseURL:            "https://theticketfairy.cloudant.com/social_data/", //'_all_docs', '100084', '_all_docs?include_docs=true&conflicts=true', '_all_docs?include_docs=true&inclusive_end=true&limit=500'
  postURL:            "https://theticketfairy.cloudant.com/social_reports/", //'_all_docs', '100084', '_all_docs?include_docs=true&conflicts=true', '_all_docs?include_docs=true&inclusive_end=true&limit=500'
  auth:               "Basic " + new Buffer("iongaintompherycastlypas" + ':' + '464b678d76b844fbaa0d6a28b17fad8dce160217').toString('base64'),

  // General options for API call
  timeOut:            10000,
  timeRetryAfter:     5000, // ms
  retryMaxLevel:      5, // 1: 5000ms, 2: 10000ms, 3: 15000ms, 4: 20000ms, 5: 25000ms

  // Numbers of calls in one parallel operation
  customerIDBatchSize:  50,
  maxCloudantConnections: 50,
  spotifyBunchCount:  50,    // maximum number of spotify bunch API call for track/artist
  spotifyAPILimit:    20,
  artistThreshold:    2,

  // Compact Threshold
  compactThreshold:   40,

  // Caching data
  // cacheArtist:        "./cache/artists/",   // cache/artist
  // cacheTrack:         "./cache/tracks/",    // cache/track
  // cacheIndexFile:     "index.json",
  cacheType_Track:    "MusicTracks",
  cacheType_Artist:   "MusicArtists",
  // cacheGroupSize:     10000,
};

const db_tracks = levelup('./leveldb-cache/tracks', { valueEncoding: 'json', cacheSize: 64 * 1024 * 1024 });
const db_artists = levelup('./leveldb-cache/artists', { valueEncoding: 'json', cacheSize: 8 * 1024 * 1024 });

// node app.js id=623 type=event section=all,gender,likes,music includeIDs=true log=true cache=true
const g_args = {
  id:             "0",            // eventID /
  type:           CONSTANTS.TYPE_EVENT,     // event / brand, ex: event = https://www.theticketfairy.com/api/customer-ids/623, brand = https://www.theticketfairy.com/api/customer-ids/138/?type=bran
  section:        CONSTANTS.SECTION_DEFAULT,    // all,gender,likes,music
  includeIDs:     "true",         // true: customerIDs in json, false: customerIDs not included in json
  log:            "true",         // true: dump log to log/type-id-section-includeIDs-timestamp.log,
  cache:          "true",        // true: enable cache for music artist/track, false: disable
};

const g_data = {
  // customerIDs from CONSTANTS.srcURL
  customerIDArray:            [],
  totalCustomerCount:         0,
  currentPos:                 0, // indicator for customerIDArray
  retryLevel:                 0,

  // variables to be stored in json
  maleCount:                  0,
  femaleCount:                0,

  likesArray:                 [],
  likesArrayIndex:            {},
  likesArrayCompacted:        [],

  spotifyApi:                 null,
  spotifyAccessToken:         "",
  currentTrackPos:            0,
  tracksArray:                [],
  tracksArrayIndex:           {},
  tracksArrayNeedFetch:       [],
  artistsArray:               [],
  artistsArrayIndex:          {},
  musicStreamingArray:        [],
  musicStreamingArrayIndex:   {},
  musicStreamingArrayNeedFetch:   [],
  musicStreamingArrayCompacted:   [],
  musicApps:                  {},
  currentSpotifyPos:          0,

  // variables for jsoncaching
  tracks_loadedCacheArray:    {},
  tracks_newCacheArray:       {},
  artists_loadedCacheArray:   {},
  artists_newCacheArray:      {},

  // variables for showing result
  fetchSuccessed:             [],
  fetchFailed:                [],
  fetchNotFound:              [],
  totalLikesLoaded:           0,
  totalTracksLoaded:          0,
  fetchArtistSuccessed:       [],
  fetchArtistFailed:          [],

  // variables for posting sections to DB
  postParams:                 [],
  currentPostPos:             0, // indicator for postParams

  // time elapsed calculation
  time_total:                 0,
  time_step2:                 0, // Loading customer ids from array
  // time_step21:                0, // Loading cached JSON (artists, tracks)
  time_step31:                0, // Fetch customers from Cloudant API
  time_step32:                0, // Fetch artists from Spotify API
  time_step33:                0, // Calculate artist fan count
  time_step34:                0, // Fetch spotify artist images
  time_step5:                 0, // cacheToJSON
  time_step6:                 0, // Post sections to DB
};

// ===== Block 0: Main Function =====
main();

function printLog(str){
  console.log(str);
}

function main(){
  const t = process.hrtime();
  async.waterfall([
    function(callback) {
      // STEP 1: Process arguments and put result to g_args
      printLog('STEP 1: Process arguments and put result to g_args');
      processArguments(callback);
    },
    function(callback) {
      // STEP 2: Load customerIDs
      printLog('STEP 2: Load customerIDs');
      loadCustomerIDs(callback);
    },
    function(callback){
      // STEP 3.1: Fetch customers from Cloudant API
      printLog('STEP 3.1: Fetch customers from Cloudant API');
      fetchCustomers(callback);
    },
    function(callback) {
      // STEP 3.2.0: Spotify Login
      printLog('STEP 3.2.0: Spotify Login');
      processSpotifyLogin(callback);
    },
    function(callback){
      // STEP 3.2: Fetch artists from Spotify API
      printLog('STEP 3.2: Fetch artists from Spotify API');
      fetchArtists(callback);
    },
    function(callback){
      // STEP 3.3: Calculate artist fan count
      printLog('STEP 3.3: Calculate artist fan count');
      calculateArtistFanCount(callback);
    },
    function(callback){
      // STEP 3.4: Fetch spotify artist images
      printLog('STEP 3.4: Fetch spotify artist images');
      fetchSpotifyArtistImages(callback);
    },
    function(callback){
      // STEP 4: Print result
      printLog('STEP 4: Print result');
      printResult(callback);
    },
    function(callback){
      // STEP 5: Cache API call
      printLog('STEP 5: Cache API call');
      cacheToJSON(callback);
    },
    function(callback){
      // STEP 6: Post sections to DB
      printLog('STEP 6: Post sections to DB');
      postSectionsToDB(callback);
    }
  ], function (err, result) {
    g_data.time_total = process.hrtime(t)[0];
    printLog('Total Time Elapsed: ' + g_data.time_total + '(s)');
    if(err){
      printLog('Error Occurred: ' + result);
      return;
    }else{
      printLog('Successfully finished!');
      return;
    }
  });
}

// ===== STEP 1: Process arguments and put result to g_args =====
function processArguments(callback){
  // process arguments
  for(let i = 0; i < process.argv.length; i++){
    const arg = process.argv[i];
    if(arg.indexOf('=') >= 0){
      const param = arg.split('=');
      g_args[param[0]] = param[1];
    }
  }

  if(g_args.log === "true"){
    // configure output
    const dir = __dirname + '/log';
    utility.checkDirectorySync(dir);
    const timestamp = new Date().toISOString().replace(/T/, '_').replace(/\..+/, '').replace(/:/g, '').replace(/-/g, '');
    const path = dir + '/' + g_args.type + '-' + g_args.id + '-' + g_args.section + '-' + g_args.includeIDs + '-' + (timestamp) + '.log';
    const log_file = fs.createWriteStream(path, {flags : 'w'});
    const log_stdout = process.stdout;
    console.log = function(d) { //
      log_file.write(util.format(d) + '\n');
      log_stdout.write(util.format(d) + '\n');
    };
  }
  // pass to next step
  callback();
}

// ===== STEP 2: Loading customer ids from array =====
function loadCustomerIDs(callback){
  const srcURL = CONSTANTS.srcURL + g_args.id + (g_args.type === CONSTANTS.TYPE_EVENT ? '' : '/?type=brand');
  printLog(' From: ' + srcURL);
  const t = process.hrtime();

  request(srcURL, function (err, resp, body) {
    if (!err && resp.statusCode === 200) {
      const resultAsJSON = JSON.parse(body);
      g_data.customerIDArray = resultAsJSON.data ? resultAsJSON.data.customerIds : [];
    }else{
      g_data.time_step2 = process.hrtime(t)[0];
      printLog(' Time Elapsed: ' + g_data.time_step2 + '(s)');

      callback(true, 'STEP 2 - HTTPRequest failed');
      return;
    }
    g_data.totalCustomerCount = g_data.customerIDArray ? g_data.customerIDArray.length : 0;
    printLog(' Loaded customers: ' + g_data.totalCustomerCount);

    g_data.time_step2 = process.hrtime(t)[0];
    printLog(' Time Elapsed: ' + g_data.time_step2 + '(s)');

    if(g_data.totalCustomerCount > 0){
      callback();
    }else{
      callback(true, 'STEP 2 - Loaded customers: 0');
    }
  });
}


// ===== STEP 3.1: Fetch customers from Cloudant API =====
function fetchCustomers(callback){
  const t = process.hrtime();
  const customerIDsToFetch = g_data.customerIDArray.slice(); // Shallow copy of global var so we don't modify the global

  // Put customer IDs into batches
  const arrayOfCustomerIDBatches = [];
  while(customerIDsToFetch.length){
    arrayOfCustomerIDBatches.push(customerIDsToFetch.splice(0, CONSTANTS.customerIDBatchSize));
  }

  // Fetch batches from cloudant
  async.forEachLimit(arrayOfCustomerIDBatches, CONSTANTS.maxCloudantConnections, fetchCustomerBatch, (err) => {
    if (err) { printLog(`Error occurred fetching customers: ${err}`); }
    // Move to next step - print result
    g_data.time_step31 = process.hrtime(t)[0];
    printLog(' Time Elapsed: ' + g_data.time_step31 + '(s)');
    return callback();
  });
}

function fetchCustomerBatch(customerIDBatch, callback) {
  printLog(` Fetching: ${customerIDBatch.length} from Cloudant`);
  const IDs = JSON.stringify(customerIDBatch);
  const requestOptions = {
    url: `${CONSTANTS.baseURL}_all_docs?include_docs=true&keys=${IDs}` ,
    json:true,
    timeout: CONSTANTS.timeOut,
    headers:{ 'Authorization': CONSTANTS.auth },
    maxAttempts: CONSTANTS.retryMaxLevel,
    retryDelay: CONSTANTS.timeRetryAfter,
  };

  requestretry(requestOptions, (err, response, data) => {
    if (err){
      printLog(`  Error fetching batch after ${CONSTANTS.retryMaxLevel} retries. Skipping IDs: ${IDs}. Error: ${err}`);
      return callback(); // Don't halt processing
    }
    else if (response.statusCode !== 200){
      printLog(`  Error fetching batch after ${CONSTANTS.retryMaxLevel} retries. Skipping IDs: ${IDs}. Status code: ${response.statusCode}`);
      return callback(); // Don't halt processing
    }
    else if (data.rows){ // 200 response
      data.rows.forEach((row) => {
        if(row.error){
          g_data.fetchNotFound.push(row.key);
          printLog(`  CustomerID: ${row.key}, Not Found. Error: ${row.error}`);
        }
        else{
          processCustomer(row.key, row.doc);
        }
      });
      return callback();
    }
    else{
      printLog(`  Error fetching batch. Skipping IDs: ${IDs}. Status code: ${response.statusCode}. Data: ${JSON.stringify(data)}`);
      return callback(); // Don't halt processing
    }
  });
}

function processCustomer(id, customer){
  const loadedLikes = customer.likes ? customer.likes.length : 0;
  const loadedMusics = customer.music_listens ? customer.music_listens.length : 0;

  const isMale = customer.profile ? customer.profile.gender === 'male' : false;
  if(isMale)
    g_data.maleCount++;
  else
    g_data.femaleCount++; // If customer.profile is missing it will default to female???  

  g_data.totalLikesLoaded += loadedLikes;
  g_data.totalTracksLoaded += loadedMusics;
  g_data.fetchSuccessed.push(id);

  appendToLikes(customer.likes, id);
  appendToTracks(customer.music_listens, id);
}

function appendToLikes(likes, customerID){
  if(!likes || likes.length === 0)
    return;
  for(let i = 0; i < likes.length; i++){
    const like = likes[i];
    const object = {
      id: like.id,
      name: like.name,
      category : like.category,
      count: 1,
      userIDs: [customerID]
    };

    let index = g_data.likesArrayIndex[object.id];
    if(index === undefined) { // if not exist
      index = g_data.likesArray.length;
      g_data.likesArrayIndex[object.id] = index;
      g_data.likesArray[index] = object;
    }else{ // duplicated
      //printLog('duplicated!', g_data.likesArray[index], object);
      g_data.likesArray[index].count++;
      g_data.likesArray[index].userIDs.push(customerID);
    }
  }
}

function appendToTracks(music_listens, customerID){
  if(!music_listens || music_listens.length === 0)
    return;
  for(let i = 0; i < music_listens.length; i++){
    const music_listen = music_listens[i];
    if(music_listen && music_listen.application && music_listen.application.name){
      const appName = music_listen.application.name;
      const index = g_data.musicApps[appName];
      if(index === undefined){
        g_data.musicApps[appName] = 1;
      }else{
        g_data.musicApps[appName]++;
      }
    }
    if(!music_listen || !music_listen.application || music_listen.application.name !== "Spotify" || !music_listen.data || !music_listen.data.song ){
      // not spotify
    }else{ // only spotify
      const songURL = music_listen.data.song.url.replace('http:', 'https:');
      let trackID;
      if (songURL.startsWith(CONSTANTS.spotifyTrackPath)){
        // e.g. "url": "https://open.spotify.com/track/7ISL3LO8AWP3fKIXunvqTa"
        trackID = songURL.replace(CONSTANTS.spotifyTrackPath, '');
      }
      else if (songURL.startsWith(CONSTANTS.spotifyAlbumPath)){
        // e.g. https://open.spotify.com/album/2wT6Qdct0caJh8nuCjxC2r/5n6sonfVlrvFUO4EGpqEcV
        trackID = songURL.replace(CONSTANTS.spotifyAlbumPath, '').split('/')[1];
      }
      else {
        printLog(`WARNING: Invalid Spotify url pattern. Skipping ${songURL}`);
        continue; // Log and skip urls that don't match the expected pattern. e.g.                 
      }
      const object = {
        trackID: trackID,
        custIDArray: [],
        custIDArrayIndex: {},
        count: 1
      };
      object.custIDArray[0] = customerID;
      object.custIDArrayIndex[customerID] = 0;

      let index = g_data.tracksArrayIndex[trackID];
      if(index === undefined) { // is not exist
        // add to array
        index = g_data.tracksArray.length;
        g_data.tracksArray[index] = object;
        g_data.tracksArrayIndex[trackID] = index;
      }else{
        // already exist
        const track = g_data.tracksArray[index];
        // check if customerID is in array
        let indexCustID = track.custIDArrayIndex[customerID];
        if(indexCustID === undefined){ // is not exist
          // add to array
          indexCustID = track.custIDArray.length;
          track.custIDArray[indexCustID] = customerID;
          track.custIDArrayIndex[customerID] = indexCustID;
        }
        track.count++;
      }
    }
  }
}

// ===== STEP 3.2.0: Spotify Login =====
function processSpotifyLogin(callback){
  // Create the api object with the credentials
  g_data.spotifyApi = new SpotifyWebApi({
    clientId : CONSTANTS.spotifyClientID,
    clientSecret : CONSTANTS.spotifyClientSecret,
  });

  // Retrieve an access token.
  g_data.spotifyApi.clientCredentialsGrant()
    .then(function(data) {
      printLog(' The access token expires in ' + data.body['expires_in']);
      printLog(' The access token is ' + data.body['access_token']);
      g_data.spotifyAccessToken = data.body['access_token'];

      // Set the access token on the API object to use it in later calls
      // g_data.spotifyApi.setAccessToken(data.body['access_token']);
      callback();
    }, function(err) {
      // console.log('Something went wrong when retrieving an access token', err);
      callback(true, "Spotify Login failed: " + err);
    });
}

// ===== STEP 3.2: Fetch artists from Spotify API =====
function fetchArtists(callback){
  printLog(' TotalCount: ' + g_data.tracksArray.length);

  fetchArtistsFromCached((err) => {
    if (err) callback(err);

    const t = process.hrtime();
    async.whilst(
      function test() {
        return g_data.currentTrackPos < g_data.tracksArrayNeedFetch.length;
      },
      fetchArtists_consequence,
      function (err) {
        // Move to next step - print result
        g_data.time_step32 = process.hrtime(t)[0];
        printLog(' Time Elapsed: ' + g_data.time_step32 + '(s)');
        callback();
      }
    );
  });
}

function fetchArtistsFromCached(fetchCallback){
  g_data.tracksArrayNeedFetch = [];
  const t = process.hrtime();
  const loadFromCached = [];

  async.each(g_data.tracksArray, (trackObj, callback) =>{
      db_tracks.get(trackObj.trackID, (err, trackData) => {
        if (err) {
          g_data.tracksArrayNeedFetch.push(trackObj);

          if (err.notFound) {
            return callback();
          }
          // I/O or other error, pass it up the callback chain
          return callback(err)
        }

        loadFromCached.push(trackData);
        g_data.fetchArtistSuccessed.push(trackObj.trackID);
        return callback();
      });
    },
    (err) => {
      if (err) {
        printLog('Error: ' + err);
      }
      appendToArtists(loadFromCached, false);
      printLog(' - Load from Cache: ' + loadFromCached.length + ', Time Elapsed: ' + process.hrtime(t)[0] + '(s)');
      printLog(' - Need Fetch: ' + g_data.tracksArrayNeedFetch.length);
      return fetchCallback();
    });
}

function fetchArtists_consequence(next){
  const start = g_data.currentTrackPos;
  const step = CONSTANTS.spotifyAPILimit * CONSTANTS.spotifyBunchCount;
  const end = g_data.currentTrackPos + step > g_data.tracksArrayNeedFetch.length ? g_data.tracksArrayNeedFetch.length : g_data.currentTrackPos + step;

  const sliced = g_data.tracksArrayNeedFetch.slice(start, end);
  if(sliced.length === 0){
    next();
    return;
  }
  printLog('  Loading: ' + sliced.length + ', start: ' + start);

  // prepare bunch trackAPI call
  const sliced1 = [];
  let ids = [];
  let count = 0;
  for(let i = 0; i < sliced.length; i++){
    ids.push(sliced[i].trackID);
    count++;
    if(count >= CONSTANTS.spotifyBunchCount || (i === sliced.length - 1)){
      const param = ids.join(",");
      const obj = {
        ids: param,
        successed: false // false: error, true: successed
      };
      sliced1.push(obj);
      ids = [];
      count = 0;
    }
  }
  // console.log(sliced1);
  g_data.retryLevel = 0;
  fetchArtists_block(sliced1, next);
}

function fetchArtists_block(array, callback){
  async.forEachOf(array,
    fetchArtists_parallel,
    function(err){
      let successed = true;
      const newArray =[];
      for(i = 0; i< array.length; i++){
        if(!array[i].successed){
          successed = false;
          newArray.push(array[i]);
        }
      }
      if(successed){
        callback();
      }else if(g_data.retryLevel >= CONSTANTS.retryMaxLevel){
        for(i = 0; i<newArray.length;i++){
          const ids = newArray[i].ids;
          const idArray = ids.split(",");
          for(let j = 0; j<idArray.length; j++)
            g_data.fetchArtistFailed.push(idArray[j]);
        }
        callback(); // terminate execution and move to parent.
      }else{
        g_data.retryLevel++;
        printLog('  Failed: ' + newArray.length);
        const retryAfter = CONSTANTS.timeRetryAfter * g_data.retryLevel;
        printLog('  Retry after: ' + retryAfter + 'ms in level: ' + g_data.retryLevel);
        setTimeout(function(){
          fetchArtists_block(newArray, callback);
        }, retryAfter);
      }
    }
  );
}

function fetchArtists_parallel(item, key, callback){
  const tracksURL = CONSTANTS.spotifyTrackAPI + "?ids=" + item.ids;
  const idArray = item.ids.split(",");
  const count = idArray.length;
  async.waterfall([
    function(subcallback) {
      const options = {
        url: tracksURL,
        timeout: CONSTANTS.timeOut,
        headers:{
          'Authorization': "Bearer " + g_data.spotifyAccessToken,
        }
      };
      request(options, function (err, resp, body) {
        if (!err && resp.statusCode === 200) {
          resp.setEncoding('utf8');
          const resultAsJSON = JSON.parse(body);
          item.successed = true;
          subcallback(null, resultAsJSON);
        }else{
          item.successed = false;
          printLog('  TracksURL: "' + tracksURL + '" Error: ' + err + ': ' + utility.filterLineBreak(body));
          // for(let i = 0; i < idArray.length; i++)
          //     g_data.fetchArtistFailed.push(idArray[i]);
          subcallback(true, '');
        }
      });
    },
    function(json, subcallback){
      for(let i = 0; i < idArray.length; i++)
        g_data.fetchArtistSuccessed.push(idArray[i]);
      appendToArtists(json.tracks, true);
      subcallback();
    }
  ], function (err, result) {
    if(g_data.retryLevel === 0)
      g_data.currentTrackPos += count;
    callback();
  });
}

function appendToArtists(tracks, saveCache){
  if(!tracks || tracks.length === 0)
    return;
  for(let ti = 0; ti < tracks.length; ti++){
    if(tracks[ti] && tracks[ti].artists){
      const artists = tracks[ti].artists;
      let trackID = tracks[ti].id;
      if(tracks[ti].linked_from) // same contents for different id
        trackID = tracks[ti].linked_from.id;
      const cachedArtists = []; // for caching
      for(let i = 0; i < artists.length; i++){
        const artist = artists[i];
        const artistID = artist.id;
        const object = {
          id: artistID,
          name: artist.name,
          application: 'Spotify',
          href: artist.href,
          trackIDArray: [],
          trackIDArrayIndex: {},
          customerFanArray: {},
          count: 1,
        };
        if(saveCache){
          const cached = {
            id: artistID,
            name: artist.name,
            application: 'Spotify',
            href: artist.href
          };
          cachedArtists.push(cached);
        }
        object.trackIDArray[0] = trackID;
        object.trackIDArrayIndex[trackID] = 0;

        let index = g_data.artistsArrayIndex[artistID];
        if(index === undefined) { // is not exist
          // add to array
          index = g_data.artistsArray.length;
          g_data.artistsArray[index] = object;
          g_data.artistsArrayIndex[artistID] = index;
        }else{
          // already exist
          const artist = g_data.artistsArray[index];
          // check if trackID is in array
          let indexTrackID = artist.trackIDArrayIndex[trackID];
          if(indexTrackID === undefined){
            // add to array
            indexTrackID = artist.trackIDArray.length;
            artist.trackIDArray[indexTrackID] = trackID;
            artist.trackIDArrayIndex[trackID] = indexTrackID;
          }
          artist.count++;
        }
      }
      // for caching
      if(saveCache){
        g_data.tracks_newCacheArray[trackID] = {
          id: trackID,
          artists: cachedArtists
        };
      }
    }
  }
}

// ===== STEP 3.3: Calculate artist fan count =====
function calculateArtistFanCount(callback){
  const t = process.hrtime();

  // calculate customerFanArray
  for(let i = 0; i < g_data.artistsArray.length; i++){
    const artist = g_data.artistsArray[i];
    for(let j = 0; j < artist.trackIDArray.length; j++){
      const trackID = artist.trackIDArray[j];
      const trackIndex = g_data.tracksArrayIndex[trackID];
      if(trackIndex !== undefined){
        const track = g_data.tracksArray[trackIndex];
        for(let k = 0; k < track.custIDArray.length; k++){
          const custID = track.custIDArray[k];
          const searchCustomer = artist.customerFanArray[custID];
          if(searchCustomer === undefined){
            artist.customerFanArray[custID] = 1;
          }else{
            artist.customerFanArray[custID]++;
          }
        }
      }
    }
  }
  // console.log(g_data.artistsArray);
  // create new array
  for(i = 0; i < g_data.artistsArray.length; i++){
    const artist = g_data.artistsArray[i];
    let fanCount = 0;
    for (const key in artist.customerFanArray) {
      const count = artist.customerFanArray[key];
      if(count >= CONSTANTS.artistThreshold){
        fanCount++;
      }
    }
    if(fanCount > 0){
      const object = {
        id: artist.id,
        name: artist.name,
        application: 'Spotify',
        href: artist.href,
        count: fanCount
      };
      const index = g_data.musicStreamingArray.length;
      g_data.musicStreamingArray[index] = object;
      g_data.musicStreamingArrayIndex[artist.id] = index;
    }
  }

  // Move to next step - fetch spotify artist images
  g_data.time_step33 = process.hrtime(t)[0];
  printLog(' Time Elapsed: ' + g_data.time_step33 + '(ms)');
  callback();
}

// ===== STEP 3.4: Fetch spotify artist images =====
function fetchSpotifyArtistImages(callback){
  printLog(' TotalCount: ' + g_data.musicStreamingArray.length);

  fetchArtistImagesFromCached((err) => {
    if (err) callback(err);

    const t = process.hrtime();
    async.whilst(
      function test() {
        return g_data.currentSpotifyPos < g_data.musicStreamingArrayNeedFetch.length;
      },
      fetchSpotifyArtist_consequence,
      function (err) {
        // Move to next step - print result
        g_data.time_step34 = process.hrtime(t)[0];
        printLog(' Time Elapsed: ' + g_data.time_step34 + '(s)');
        callback();
      }
    );
  });
}


function fetchArtistImagesFromCached(fetchCallback) {
  g_data.musicStreamingArrayNeedFetch = [];
  const t = process.hrtime();
  const totalCount = g_data.musicStreamingArray.length;
  const loadFromCached = [];

  async.each(g_data.musicStreamingArray, (artistObj, callback) =>{
      db_artists.get(artistObj.id, (err, artistData) => {
        if (err) {
          g_data.musicStreamingArrayNeedFetch.push(artistObj);

          if (err.notFound) {
            return callback();
          }
          // I/O or other error, pass it up the callback chain
          return callback(err)
        }

        loadFromCached.push(artistData);
        return callback();
      });
    },
    (err) => {
      if (err) {
        printLog('Error: ' + err);
      }
      appendImagesToArtists(loadFromCached, false);
      printLog(' - Load from Cache: ' + loadFromCached.length + ', Time Elapsed: ' + process.hrtime(t)[0] + '(s)');
      printLog(' - Need Fetch: ' + g_data.musicStreamingArrayNeedFetch.length);
      return fetchCallback();
    });
}

function fetchSpotifyArtist_consequence(next) {
  const start = g_data.currentSpotifyPos;
  const step = CONSTANTS.spotifyAPILimit * CONSTANTS.spotifyBunchCount;
  const end = g_data.currentSpotifyPos + step > g_data.musicStreamingArrayNeedFetch.length ? g_data.musicStreamingArrayNeedFetch.length : g_data.currentSpotifyPos + step;

  const sliced = g_data.musicStreamingArrayNeedFetch.slice(start, end);
  if(sliced.length === 0){
    next();
    return;
  }
  printLog('  Loading: ' + sliced.length + ', start: ' + start);

  // prepare bunch trackAPI call
  const sliced1 = [];
  let ids = [];
  let count = 0;
  for(let i = 0; i < sliced.length; i++){
    ids.push(sliced[i].id);
    count++;
    if(count >= CONSTANTS.spotifyBunchCount || (i === sliced.length - 1)){
      const param = ids.join(",");
      const obj = {
        ids: param,
        successed: false // false: error, true: successed
      };
      sliced1.push(obj);
      ids = [];
      count = 0;
    }
  }
  // console.log(sliced1);
  g_data.retryLevel = 0;
  fetchSpotifyArtist_block(sliced1, next);
}

function fetchSpotifyArtist_block(array, callback){
  async.forEachOf(array,
    fetchSpotifyArtist_parallel,
    function(err){
      let successed = true;
      const newArray =[];
      for(i = 0; i< array.length; i++){
        if(!array[i].successed){
          successed = false;
          newArray.push(array[i]);
        }
      }
      if(successed || g_data.retryLevel >= CONSTANTS.retryMaxLevel){
        callback(); // terminate execution and move to parent.
      }
      else{
        g_data.retryLevel++;
        printLog('  Failed: ' + newArray.length);
        const retryAfter = CONSTANTS.timeRetryAfter * g_data.retryLevel;
        printLog('  Retry after: ' + retryAfter + 'ms in level: ' + g_data.retryLevel);
        setTimeout(function(){
          fetchSpotifyArtist_block(newArray, callback);
        }, retryAfter);
      }
    }
  );
}

function fetchSpotifyArtist_parallel(item, key, callback){
  const artistsURL = CONSTANTS.spotifyArtistAPI + "?ids=" + item.ids;
  const idArray = item.ids.split(",");
  const count = idArray.length;
  async.waterfall([
    function(subcallback) {
      const options = {
        url: artistsURL,
        timeout: CONSTANTS.timeOut,
        headers:{
          'Authorization': "Bearer " + g_data.spotifyAccessToken,
        }
      };
      request(options, function (err, resp, body) {
        if (!err && resp.statusCode === 200) {
          resp.setEncoding('utf8');
          const resultAsJSON = JSON.parse(body);
          item.successed = true;
          subcallback(null, resultAsJSON);
        }else{
          item.successed = false;
          printLog('  ArtistsURL: "' + artistsURL + '" Error: ' + err + ': ' + utility.filterLineBreak(body));
          subcallback(true, '');
        }
      });
    },
    function(json, subcallback){
      // console.log(json.images);
      appendImagesToArtists(json.artists, true);
      subcallback();
    }
  ], function (err, result) {
    if(g_data.retryLevel === 0)
      g_data.currentSpotifyPos += count;
    callback();
  });
}

function appendImagesToArtists(artists, saveCache){
  if(!artists || artists.length === 0)
    return;
  for(let i = 0; i< artists.length; i++){
    if(artists[i]){
      const id = artists[i].id;
      const index = g_data.musicStreamingArrayIndex[id];
      if(index !== undefined){
        const obj = g_data.musicStreamingArray[index];
        obj.images = artists[i].images;
        if(saveCache){
          g_data.artists_newCacheArray[id] = {
            id: id,
            name: obj.name,
            application: 'Spotify',
            href: obj.href,
            images: obj.images
          };
        }
      }

    }
  }
}

// ===== STEP 4: Post result =====
function printResult(callback){
  g_data.likesArrayCompacted = getCompactedArray(g_data.likesArray);
  g_data.musicStreamingArrayCompacted = getCompactedArray(g_data.musicStreamingArray);
  printLog(' Total Customer Fetched: ' + g_data.totalCustomerCount);
  printLog(' - Success: ' + g_data.fetchSuccessed.length + ', male: ' + g_data.maleCount + ', female: ' + g_data.femaleCount);
  printLog(' - Not Found: ' + g_data.fetchNotFound.length + ', ids: ' + g_data.fetchNotFound.toString());
  printLog(' - Failed: ' + g_data.fetchFailed.length + ', ids: ' + g_data.fetchFailed.toString());
  printLog(' Total Likes Fetched: ' + g_data.totalLikesLoaded + ', Unique Count: ' + g_data.likesArray.length + ', Compacted Count: ' + g_data.likesArrayCompacted.length);
  printLog(' Total Tracks Fetched: ' + g_data.totalTracksLoaded + ', Unique Count: ' + g_data.tracksArray.length);
  printLog(' - Success: ' + g_data.fetchArtistSuccessed.length);
  printLog(' - Failed: ' + g_data.fetchArtistFailed.length + ', ids: ' + g_data.fetchArtistFailed.toString());
  printLog(' Total Artists Fetched: ' + g_data.fetchArtistSuccessed.length + ', Unique Count: ' + g_data.artistsArray.length);
  printLog(' Total MusicListens Fetched: ' + g_data.artistsArray.length + ', Filtered Count: ' + g_data.musicStreamingArray.length + ', Compacted Count: ' + g_data.musicStreamingArrayCompacted.length);
  printLog(' Music Apps: ' + JSON.stringify(g_data.musicApps));
  callback();
}

function getCompactedArray(srcArray){
  const compactedArray = [];
  for(let i=0;i<srcArray.length;i++){
    const obj = srcArray[i];
    if(obj && obj.count >= CONSTANTS.compactThreshold){
      compactedArray.push(obj);
    }
  }
  return compactedArray;
}

// ===== STEP 5: Cache API call =====
function cacheToJSON(callback){
  const t = process.hrtime();
  if(g_args.cache === "true"){
    cacheData(CONSTANTS.cacheType_Track, function(){
      cacheData(CONSTANTS.cacheType_Artist, function(){
        db_tracks.close((err) => {
          if (err) printLog(`Error closing db_tracks: ${err}`);
          db_artists.close((err) => {
            if (err) printLog(`Error closing db_artists: ${err}`);
            g_data.time_step5 = process.hrtime(t)[0];
            printLog(' Time Elapsed: ' + g_data.time_step5 + '(s)');
            callback();
          });
        });
      });
    });
  }else{
    printLog(' Cache not enabled.');
    callback();
  }
}

function cacheData(type, callback){
  const newCache = (type === CONSTANTS.cacheType_Track) ? g_data.tracks_newCacheArray : g_data.artists_newCacheArray;
  const newCacheCount = Object.keys(newCache).length;

  if (newCacheCount === 0){
    printLog(' Caching ' + type + ': NO NEW RECORDS TO CACHE.');
    return callback();
  }

  const db = (type === CONSTANTS.cacheType_Track) ? db_tracks : db_artists;
  const batch = db.batch();
  for (let key in newCache) {
    if (newCache.hasOwnProperty(key)) {
      batch.put(key, newCache[key]);
    }
  }
  batch.write((err) => {
    if (err) { return callback(err); }

    printLog(' Caching ' + type + ' Count: ' + newCacheCount +' newly cached, Path:' + db.location + ', Result:' + (err ? err : 'success.'));
    return callback();
  });
}

// ===== STEP 6: Post sections to DB =====
function postSectionsToDB(callback){
  const t = process.hrtime();

  // prepare section array to be dealt with
  const sectionArray = [];
  const sections = g_args.section.split(CONSTANTS.SECTION_SPLITTER);
  for(let i = 0; i < sections.length; i++){
    sectionArray.push({section: sections[i], compacted: true});
    sectionArray.push({section: sections[i], compacted: false});
  }

  async.each(sectionArray, postSectionToDB, (err) => {
      g_data.time_step6 = process.hrtime(t)[0];
      printLog(' Time Elapsed: ' + g_data.time_step6 + '(s)');
      callback();
    }
  );
}

function prepareJSON(section, compacted){
  // compacted(true: filtered by threshold, false: not filtered)
  let ret = {
    _id:    g_args.type + '-' + g_args.id + '-' + section + (compacted ? '-compact' : ''),
    type:   g_args.type,
    data:   {}
  };

  const timestamp = new Date().toISOString();
  ret.generated_at = timestamp;  

  if(g_args.includeIDs === "true")
    ret.data.customerIDs = g_data.customerIDArray;
  if(section === CONSTANTS.SECTION_ALL || section === CONSTANTS.SECTION_GENDER)
    ret.data.gender = { male: g_data.maleCount, female: g_data.femaleCount };
  if(section === CONSTANTS.SECTION_ALL || section === CONSTANTS.SECTION_LIKES){
    ret.data.likes = compacted ? g_data.likesArrayCompacted : g_data.likesArray;
  }
  if(section === CONSTANTS.SECTION_ALL || section === CONSTANTS.SECTION_MUSIC){
    ret.data.musicstreaming = compacted ? g_data.musicStreamingArrayCompacted : g_data.musicStreamingArray;
  }
  return ret;
}

function postSectionToDB(section, callback){
  printLog(' Section: ' + section.section + ', Compacted: ' + (section.compacted ? 'true' : 'false'));

  // prepare JSON to be uploaded
  const t = process.hrtime();
  const postJSON = prepareJSON(section.section, section.compacted);

  async.waterfall([
    function (subcallback){
      // ----- check existence            
      const srcURL = CONSTANTS.postURL + '_all_docs?keys=["' + postJSON._id + '"]';
      printLog(' - Check if db exists already ' + srcURL);
      const options = {
        url: srcURL,
        timeout: CONSTANTS.timeOut,
        headers:{
          'Authorization': CONSTANTS.auth
        }
      };
      request(options, function (err, resp, body) {
        if (!err && resp.statusCode === 200) {
          const resultAsJSON = JSON.parse(body);
          const rev = resultAsJSON.rows[0].value;
          if(rev){
            postJSON._rev = rev.rev;
            printLog(' - Found document: ' + postJSON._id + ', rev: ' + postJSON._rev + ', overwriting...');
          }else{
            printLog(' - Not found document: ' + postJSON._id + ', creating new...');
          }
        }else{
          printLog(' * HTTP Request error: ' + postJSON._id + ', creating new...');
          //printLog(" - Response Body: " + JSON.stringify(body));
        }
        subcallback(null);
      });
    },
    function (subcallback){
      printLog(' - Posting To: ' + CONSTANTS.postURL + postJSON._id);

      zlib.gzip(JSON.stringify(postJSON), function (err, compressedJSON) {
        if (err){
          printLog(`Error compressing json to POST to DB: ${err}`);
          return subcallback(err);
        }

        const options = {
          url:    CONSTANTS.postURL,
          method: 'POST',
          body:   compressedJSON,
          headers:{
            'Authorization': CONSTANTS.auth,
            'content-type': "application/json",
            'Content-Encoding': 'gzip'
          }
        };
        request(options, function (err, resp, body) {
          printLog(' - Time Elapsed: ' + process.hrtime(t)[0] + '(s)');
          if (!err && (resp.statusCode === 200 || resp.statusCode === 201)) {
            subcallback(null);
          }
          else if (resp){
            printLog(' - Resp status: ' + resp.statusCode);
            printLog('err= ' + err +'resp=' + JSON.stringify(resp) + ' body=' + JSON.stringify(body));
            subcallback(true, err);
          }
          else {
            printLog('err= ' + err);
            subcallback(true, err);
          }
        });
      });
    }
  ], function (err, result) {
    if(err){
      printLog(' * Error: ' + err + ',' + result);
    }
    callback();
  });
}
