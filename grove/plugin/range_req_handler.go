package plugin

/*
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/apache/incubator-trafficcontrol/grove/web"
	"github.com/apache/incubator-trafficcontrol/lib/go-log"
	"io/ioutil"
	"sync"
)

type ByteRangeInfo struct {
	Start   int64
	End     int64
	IsSlice bool
}

type RequestInfo struct {
	RequestedRanges []ByteRangeInfo
	//SlicesNeeded     []ByteRangeInfo
	OriginalCacheKey string
}

const SSIZE = 1024
const SLICEKEYSTRING = "grove_range_req_handler_plugin_data"

type rangeRequestConfig struct {
	Mode              string `json:"mode"`
	MultiPartBoundary string // not in the json
}

func init() {
	AddPlugin(10000, Funcs{
		load:                rangeReqHandleLoad,
		onRequest:           rangeReqHandlerOnRequest,
		beforeCacheLookUp:   rangeReqHandleBeforeCacheLookup,
		beforeParentRequest: rangeReqHandleBeforeParent,
		beforeRespond:       rangeReqHandleBeforeRespond,
	})
}

// rangeReqHandleLoad loads the configuration
func rangeReqHandleLoad(b json.RawMessage) interface{} {
	cfg := rangeRequestConfig{}
	log.Errorf("rangeReqHandleLoad loading: %s\n", b)

	err := json.Unmarshal(b, &cfg)
	if err != nil {
		log.Errorln("range_rew_handler  loading config, unmarshalling JSON: " + err.Error())
		return nil
	}
	//if !(cfg.Mode == "get_full_serve_range" || cfg.Mode == "patch") {
	//	log.Errorf("Unknown mode for range_req_handler plugin: %s\n", cfg.Mode)
	//}

	multipartBoundaryBytes := make([]byte, 16)
	if _, err := rand.Read(multipartBoundaryBytes); err != nil {
		log.Errorf("Error with rand.Read: %v\n", err)
	}
	// Use UUID format
	cfg.MultiPartBoundary = fmt.Sprintf("%x-%x-%x-%x-%x", multipartBoundaryBytes[0:4], multipartBoundaryBytes[4:6], multipartBoundaryBytes[6:8], multipartBoundaryBytes[8:10], multipartBoundaryBytes[10:])

	log.Debugf("range_rew_handler: load success: %+v\n", cfg)
	return &cfg
}

// rangeReqHandlerOnRequest determines if there is a Range header, and puts the ranges in *d.Context as a []byteRanges
func rangeReqHandlerOnRequest(icfg interface{}, d OnRequestData) bool {
	rHeader := d.R.Header.Get("Range")
	if rHeader == "" {
		log.Debugf("No Range header found\n")
		return false
	}
	log.Debugf("Range string is: %s\n", rHeader)
	// put the ranges [] in the context so we can use it later
	byteRanges := parseRangeHeader(rHeader)

	if len(byteRanges) == 1 && byteRanges[0].Start%SSIZE == 0 && (byteRanges[0].Start+SSIZE-1) == byteRanges[0].End {
		//log.Debugf("Leaving untouched: range %-%d", thisRange.Start, thisRange.End)
		byteRanges[0].IsSlice = true
	}
	*d.Context = RequestInfo{byteRanges, ""}
	return false
}

// rangeReqHandleBeforeCacheLookup is used to override the cacheKey when in store_ranges mode,
// or to do the main slicling work in slice mode.
func rangeReqHandleBeforeCacheLookup(icfg interface{}, d BeforeCacheLookUpData) {
	cfg, ok := icfg.(*rangeRequestConfig)
	if !ok {
		log.Errorf("range_req_handler config '%v' type '%T' expected *rangeRequestConfig\n", icfg, icfg)
		return
	}

	ictx := d.Context
	ctx, ok := (*ictx).(RequestInfo)
	if !ok {
		log.Errorf("Invalid context: %v\n", ictx)
	}
	if len(ctx.RequestedRanges) == 0 {
		return // there was no (valid) range header
	}

	if cfg.Mode == "store_ranges" {
		// TODO JvD use rangeCacheKey func ?? No. that only works for one range
		sep := "?"
		if strings.Contains(d.DefaultCacheKey, "?") {
			sep = "&"
		}
		newKey := d.DefaultCacheKey + sep + SLICEKEYSTRING + d.Req.Header.Get("Range")
		d.CacheKeyOverrideFunc(newKey)
		log.Debugf("range_req_handler: store_ranges default key:%s, new key:%s\n", d.DefaultCacheKey, newKey)
		return
	}

	if cfg.Mode == "slice" { // not needed for the last one, but makes things more readable.
		if len(ctx.RequestedRanges) > 1 {
			log.Errorf("multipart ranges not supported in slice mode (yet?), results are undetermined")
			return
		}
		if ctx.RequestedRanges[0].IsSlice { // if the request is already a slice, just mod the cachekey and move on.
			sep := "?"
			if strings.Contains(d.DefaultCacheKey, "?") {
				sep = "&"
			}
			newKey := d.DefaultCacheKey + sep + SLICEKEYSTRING + d.Req.Header.Get("Range")
			d.CacheKeyOverrideFunc(newKey)
			log.Debugf("SLICE range_req_handler: store_ranges default key:%s, new key:%s\n", d.DefaultCacheKey, newKey)
			return
		}

		// --
		thisRange := ctx.RequestedRanges[0]
		firstSlice := int64(thisRange.Start / SSIZE)
		lastSlice := int64((thisRange.End / SSIZE) + 1)
		requestList := make([]*http.Request, 0)
		for i := firstSlice; i < lastSlice; i++ {

			bRange := ByteRangeInfo{i * SSIZE, ((i + 1) * SSIZE) - 1, true}

			key := d.DefaultCacheKey + rangeCacheKey(ctx.OriginalCacheKey, bRange)
			_, ok := d.Cache.Get(key)
			if ok { // Get will put it at the top of the LRU. Use Peek in Respond to build response, and not increase the hitCount anymore
				log.Debugf("SLICE URL HIT for key: %s\n", key)
				continue
			}
			URL := "http://localhost:8080" + d.Req.RequestURI
			log.Debugf("SLICE URL MISS for key: %s queuing GET for %s\n", key, URL)
			req, err := http.NewRequest("GET", URL, nil)
			if err != nil {
				log.Errorf("ERROR") // TODO
			}
			req.Host = d.Req.Host
			req.Header.Set("Range", "bytes="+strconv.FormatInt(bRange.Start, 10)+"-"+strconv.FormatInt(bRange.End, 10))
			requestList = append(requestList, req)
		}

		start := firstSlice * SSIZE
		end := ((firstSlice + 1) * SSIZE) - 1
		bRange := ByteRangeInfo{start, end, true}
		newKey := rangeCacheKey(d.DefaultCacheKey, bRange)
		d.CacheKeyOverrideFunc(newKey)
		log.Debugf("SLICE original request: range_req_handler: store_ranges default key:%s, new key:%s\n", d.DefaultCacheKey, newKey)

		if len(requestList) == 0 {
			return // it is a HIT for all slices
		}

		// change the original request to be the first slice needed
		d.Req.Header.Set("Range", requestList[0].Header.Get("Range"))
		if len(requestList) == 1 { // if there is only one, we just change the existing upstream request and are done
			return
		}

		log.Debugf("SLICE Starting child requests")
		client := &http.Client{}
		var wg sync.WaitGroup
		// do all requests except the first, since that one is handled by the original request
		for i := 1; i < len(requestList); i++ {
			wg.Add(1)
			go func(request *http.Request) {
				defer wg.Done()
				_, err := client.Do(request)
				if err != nil {
					log.Errorf("Error in slicer:%v\n", err)
				}
			}(requestList[i])
		}
		wg.Wait()
		log.Debugf("SLICE All done.")
		// --

	}
}

// rangeReqHandleBeforeParent changes the parent request if needed (mode == get_full_serve_range)
func rangeReqHandleBeforeParent(icfg interface{}, d BeforeParentRequestData) {
	log.Debugf("rangeReqHandleBeforeParent calling.")
	rHeader := d.Req.Header.Get("Range")
	if rHeader == "" {
		log.Debugln("No Range header found")
		return
	}
	log.Debugf("Range string is: %s\n", rHeader)
	cfg, ok := icfg.(*rangeRequestConfig)
	if !ok {
		log.Errorf("range_req_handler config '%v' type '%T' expected *rangeRequestConfig\n", icfg, icfg)
		return
	}

	if cfg.Mode == "get_full_serve_range" {
		// get_full_serve_range means get the whole thing from parent/org, but serve the requested range. Just remove the Range header from the upstream request
		d.Req.Header.Del("Range")
		return
	}

	//ictx := d.Context
	//ctx, ok := (*ictx).(RequestInfo)
	//if !ok {
	//	log.Errorf("Invalid context: %v\n", ictx)
	//}
	//if len(ctx.RequestedRanges) == 0 {
	//	return // there was no (valid) range header // shouldn't get here.
	//}
	//if cfg.Mode == "slice" {
	//	if len(ctx.RequestedRanges) > 1 {
	//		log.Errorf("multipart ranges not supported in slice mode (yet?), results are undetermined")
	//		return
	//	}
	//	thisRange := ctx.RequestedRanges[0]
	//	if thisRange.IsSlice {
	//		log.Debugf("Leaving untouched: range %-%d", thisRange.Start, thisRange.End)
	//		return // it's already sliced exatly how we like it.
	//	}
	//	//firstSlice := int64(thisRange.Start / SSIZE)
	//	//lastSlice := int64((thisRange.End / SSIZE) + 1)
	//	//
	//	//// TODO JvD: stash in ctx?
	//	////defCacheKey
	//	//requestList := make([]*http.Request, 0)
	//	//for i := firstSlice; i < lastSlice; i++ {
	//	//	// TODO if already in cache continue TODO
	//	//
	//	//	bRange := ByteRangeInfo{i * SSIZE, ((i + 1) * SSIZE) - 1, true}
	//	//	log.Debugf("URL: %v/%s, Range: %d-%d", d.Req, d.Req.RequestURI, bRange.Start, bRange.End)
	//	//
	//	//	URL := "http://localhost:8080" + d.Req.RequestURI
	//	//	req, err := http.NewRequest("GET", URL, nil)
	//	//	if err != nil {
	//	//		log.Errorf("ERROR")
	//	//	}
	//	//	req.Host = d.Req.Host
	//	//	req.Header.Set("Range", "bytes="+strconv.FormatInt(bRange.Start, 10)+"-"+strconv.FormatInt(bRange.End, 10))
	//	//	requestList = append(requestList, req)
	//	//}
	//	//log.Debugf("Getting: %v", requestList)
	//	//
	//	//// TODO what to do when len(requestList) == 0??  (a full HIT).
	//	//
	//	//d.Req.Header.Set("Range", requestList[0].Header.Get("Range"))
	//	////setCacheKeyForRange(d)
	//	//if len(requestList) == 1 { // if there is one, we just change the existing upstream request and are done
	//	//	return
	//	//}
	//	//
	//	//log.Debugf("Starting child requests")
	//	//client := &http.Client{}
	//	//var wg sync.WaitGroup
	//	//for i := 1; i < len(requestList); i++ {
	//	//	wg.Add(1)
	//	//	go func(request *http.Request) {
	//	//		defer wg.Done()
	//	//		_, err := client.Do(request)
	//	//		if err != nil {
	//	//			log.Errorf("Error in slicer:%v\n", err)
	//	//		}
	//	//	}(requestList[i])
	//	//}
	//	//wg.Wait()
	//	//log.Debugf("All done.")
	//}
	return
}

// rangeReqHandleBeforeRespond builds the 206 response
// Assume all the needed ranges have been put in cache before, which is the truth for "get_full_serve_range" mode which gets the whole object into cache.
// If mode == store_ranges, do nothing, we just return the object stored-as is
func rangeReqHandleBeforeRespond(icfg interface{}, d BeforeRespondData) {
	log.Debugf("rangeReqHandleBeforeRespond calling\n")
	ictx := d.Context
	ctx, ok := (*ictx).(RequestInfo)
	if !ok {
		log.Errorf("Invalid context: %v\n", ictx)
	}
	if len(ctx.RequestedRanges) == 0 {
		return // there was no (valid) range header
	}

	cfg, ok := icfg.(*rangeRequestConfig)
	if !ok {
		log.Errorf("range_req_handler config '%v' type '%T' expected *rangeRequestConfig\n", icfg, icfg)
		return
	}
	if cfg.Mode == "store_ranges" {
		return // no need to do anything here.
	}

	if cfg.Mode == "slice" {
		return //  TODO build a body
	}

	// mode != store_ranges
	multipartBoundaryString := cfg.MultiPartBoundary
	multipart := false
	originalContentType := d.Hdr.Get("Content-type")
	*d.Hdr = web.CopyHeader(*d.Hdr) // copy the headers, we don't want to mod the cacheObj
	if len(ctx.RequestedRanges) > 1 {
		multipart = true
		multipartBoundaryString = cfg.MultiPartBoundary
		d.Hdr.Set("Content-Type", fmt.Sprintf("multipart/byteranges; boundary=%s", multipartBoundaryString))
	}
	totalContentLength, err := strconv.ParseInt(d.Hdr.Get("Content-Length"), 10, 64)
	if err != nil {
		log.Errorf("Invalid Content-Length header: %v\n", d.Hdr.Get("Content-Length"))
	}
	body := make([]byte, 0)
	for _, thisRange := range ctx.RequestedRanges {
		if thisRange.End == -1 || thisRange.End >= totalContentLength { // if the end range is "", or too large serve until the end
			thisRange.End = totalContentLength - 1
		}

		rangeString := "bytes " + strconv.FormatInt(thisRange.Start, 10) + "-" + strconv.FormatInt(thisRange.End, 10) + "\r\n\r\n"
		log.Debugf("range:%d-%d\n", thisRange.Start, thisRange.End)
		if multipart {
			body = append(body, []byte("\r\n--"+multipartBoundaryString+"\r\n")...)
			body = append(body, []byte("Content-type: "+originalContentType+"%s\r\n")...)
			body = append(body, []byte("Content-range: "+rangeString)...)
		} else {
			d.Hdr.Add("Content-Range", rangeString+"/"+strconv.FormatInt(totalContentLength, 10))
		}
		bSlice := (*d.Body)[thisRange.Start : thisRange.End+1]
		body = append(body, bSlice...)
	}
	if multipartBoundaryString != "" {
		body = append(body, []byte(fmt.Sprintf("\r\n--%s--\r\n", multipartBoundaryString))...)
	}
	d.Hdr.Set("Content-Length", fmt.Sprintf("%d", len(body)))
	*d.Body = body
	*d.Code = http.StatusPartialContent
	return
}

func rangeCacheKey(defaultKey string, r ByteRangeInfo) string {
	sep := "?"
	if strings.Contains(defaultKey, "?") {
		sep = "&"
	}
	key := defaultKey + sep + SLICEKEYSTRING + "bytes=" + strconv.FormatInt(r.Start, 10) + "-" + strconv.FormatInt(r.End, 10)
	return key
}

// TODO JvD: implement
//func setCacheKeyForRange(d BeforeCacheLookUpData) {
//
//}

func parseRange(rangeString string) (ByteRangeInfo, error) {
	parts := strings.Split(rangeString, "-")

	var bRange ByteRangeInfo
	if parts[0] == "" {
		bRange.Start = 0
	} else {
		start, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			log.Errorf("Error converting rangeString start \"%\" to numbers\n", rangeString)
			return ByteRangeInfo{}, err
		}
		bRange.Start = start
	}
	if parts[1] == "" {
		bRange.End = -1 // -1 means till the end
	} else {
		end, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			log.Errorf("Error converting rangeString end \"%\" to numbers\n", rangeString)
			return ByteRangeInfo{}, err
		}
		bRange.End = end
	}
	return bRange, nil
}

func parseRangeHeader(rHdrVal string) []ByteRangeInfo {
	byteRanges := make([]ByteRangeInfo, 0)
	rangeStringParts := strings.Split(rHdrVal, "=")
	if rangeStringParts[0] != "bytes" {
		log.Errorf("Not a valid Range type: \"%s\"\n", rangeStringParts[0])
	}

	for _, thisRangeString := range strings.Split(rangeStringParts[1], ",") {
		thisRange, err := parseRange(thisRangeString)
		if err != nil {
			return nil
		}
		byteRanges = append(byteRanges, thisRange)
	}
	return byteRanges
}

// --
type responseType struct {
	Headers http.Header
	Body    []byte
}

func httpGet(URL, headers string) responseType {
	client := &http.Client{}
	req, err := http.NewRequest("GET", URL, nil)
	if err != nil {
		// TODO return error
		log.Debugln("ERROR in httpGet")
	}
	for _, hdrString := range strings.Split(headers, " ") {
		if hdrString == "" {
			continue
		}
		parts := strings.Split(hdrString, ":")
		if parts[0] == "Host" {
			req.Host = parts[1]
		} else {
			//log.Println("> ", parts)
			req.Header.Set(parts[0], parts[1])
		}
	}
	//log.Printf(">>>> %v", req)
	resp, err := client.Do(req)
	if err != nil {
		// TODO error
		log.Debugln("ERROR in httpGet")
	}
	defer resp.Body.Close()
	var response responseType
	response.Headers = web.CopyHeader(resp.Header)
	response.Body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Debugln("ERROR in httpGet")
	}
	return response
}
