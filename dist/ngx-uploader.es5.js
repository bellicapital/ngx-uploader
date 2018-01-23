import { Directive, ElementRef, EventEmitter, HostListener, Input, NgModule, Output } from '@angular/core';
import { Observable as Observable$1 } from 'rxjs/Observable';
import { Subject as Subject$1 } from 'rxjs/Subject';
import 'rxjs/add/operator/mergeMap';
var UploadStatus = {};
UploadStatus.Queue = 0;
UploadStatus.Uploading = 1;
UploadStatus.Done = 2;
UploadStatus.Cancelled = 3;
UploadStatus[UploadStatus.Queue] = "Queue";
UploadStatus[UploadStatus.Uploading] = "Uploading";
UploadStatus[UploadStatus.Done] = "Done";
UploadStatus[UploadStatus.Cancelled] = "Cancelled";
/**
 * @param {?} bytes
 * @return {?}
 */
function humanizeBytes(bytes) {
    if (bytes === 0) {
        return '0 Byte';
    }
    var /** @type {?} */ k = 1024;
    var /** @type {?} */ sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB'];
    var /** @type {?} */ i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}
var NgUploaderService = /** @class */ (function () {
    /**
     * @param {?=} concurrency
     * @param {?=} contentTypes
     */
    function NgUploaderService(concurrency, contentTypes) {
        if (concurrency === void 0) { concurrency = Number.POSITIVE_INFINITY; }
        if (contentTypes === void 0) { contentTypes = ['*']; }
        var _this = this;
        this.queue = [];
        this.serviceEvents = new EventEmitter();
        this.uploadScheduler = new Subject$1();
        this.subs = [];
        this.contentTypes = contentTypes;
        this.uploadScheduler
            .mergeMap(function (upload) { return _this.startUpload(upload); }, concurrency)
            .subscribe(function (uploadOutput) { return _this.serviceEvents.emit(uploadOutput); });
    }
    /**
     * @param {?} incomingFiles
     * @return {?}
     */
    NgUploaderService.prototype.handleFiles = function (incomingFiles) {
        var _this = this;
        var /** @type {?} */ allowedIncomingFiles = [].reduce.call(incomingFiles, function (acc, checkFile, i) {
            if (_this.isContentTypeAllowed(checkFile.type)) {
                acc = acc.concat(checkFile);
            }
            else {
                var /** @type {?} */ rejectedFile = _this.makeUploadFile(checkFile, i);
                _this.serviceEvents.emit({ type: 'rejected', file: rejectedFile });
            }
            return acc;
        }, []);
        (_a = this.queue).push.apply(_a, [].map.call(allowedIncomingFiles, function (file, i) {
            var /** @type {?} */ uploadFile = _this.makeUploadFile(file, i);
            _this.serviceEvents.emit({ type: 'addedToQueue', file: uploadFile });
            return uploadFile;
        }));
        this.serviceEvents.emit({ type: 'allAddedToQueue' });
        var _a;
    };
    /**
     * @param {?} input
     * @return {?}
     */
    NgUploaderService.prototype.initInputEvents = function (input) {
        var _this = this;
        return input.subscribe(function (event) {
            switch (event.type) {
                case 'uploadFile':
                    var /** @type {?} */ uploadFileIndex = _this.queue.findIndex(function (file) { return file === event.file; });
                    if (uploadFileIndex !== -1 && event.file) {
                        _this.uploadScheduler.next({ file: _this.queue[uploadFileIndex], event: event });
                    }
                    break;
                case 'uploadAll':
                    var /** @type {?} */ files = _this.queue.filter(function (file) { return file.progress.status === UploadStatus.Queue; });
                    files.forEach(function (file) { return _this.uploadScheduler.next({ file: file, event: event }); });
                    break;
                case 'cancel':
                    var /** @type {?} */ id_1 = event.id || null;
                    if (!id_1) {
                        return;
                    }
                    var /** @type {?} */ index = _this.subs.findIndex(function (sub) { return sub.id === id_1; });
                    if (index !== -1 && _this.subs[index].sub) {
                        _this.subs[index].sub.unsubscribe();
                        var /** @type {?} */ fileIndex = _this.queue.findIndex(function (file) { return file.id === id_1; });
                        if (fileIndex !== -1) {
                            _this.queue[fileIndex].progress.status = UploadStatus.Cancelled;
                            _this.serviceEvents.emit({ type: 'cancelled', file: _this.queue[fileIndex] });
                        }
                    }
                    break;
                case 'cancelAll':
                    _this.subs.forEach(function (sub) {
                        if (sub.sub) {
                            sub.sub.unsubscribe();
                        }
                        var /** @type {?} */ file = _this.queue.find(function (uploadFile) { return uploadFile.id === sub.id; });
                        if (file) {
                            file.progress.status = UploadStatus.Cancelled;
                            _this.serviceEvents.emit({ type: 'cancelled', file: file });
                        }
                    });
                    break;
                case 'remove':
                    if (!event.id) {
                        return;
                    }
                    var /** @type {?} */ i = _this.queue.findIndex(function (file) { return file.id === event.id; });
                    if (i !== -1) {
                        var /** @type {?} */ file = _this.queue[i];
                        _this.queue.splice(i, 1);
                        _this.serviceEvents.emit({ type: 'removed', file: file });
                    }
                    break;
                case 'removeAll':
                    if (_this.queue.length) {
                        _this.queue = [];
                        _this.serviceEvents.emit({ type: 'removedAll' });
                    }
                    break;
            }
        });
    };
    /**
     * @param {?} upload
     * @return {?}
     */
    NgUploaderService.prototype.startUpload = function (upload) {
        var _this = this;
        return new Observable$1(function (observer) {
            var /** @type {?} */ sub = _this.uploadFile(upload.file, upload.event)
                .subscribe(function (output) {
                observer.next(output);
            }, function (err) {
                observer.error(err);
                observer.complete();
            }, function () {
                observer.complete();
            });
            _this.subs.push({ id: upload.file.id, sub: sub });
        });
    };
    /**
     * @param {?} file
     * @param {?} event
     * @return {?}
     */
    NgUploaderService.prototype.uploadFile = function (file, event) {
        var _this = this;
        return new Observable$1(function (observer) {
            var /** @type {?} */ url = event.url || '';
            var /** @type {?} */ method = event.method || 'POST';
            var /** @type {?} */ data = event.data || {};
            var /** @type {?} */ headers = event.headers || {};
            var /** @type {?} */ xhr = new XMLHttpRequest();
            var /** @type {?} */ time = new Date().getTime();
            var /** @type {?} */ progressStartTime = (file.progress.data && file.progress.data.startTime) || time;
            var /** @type {?} */ speed = 0;
            var /** @type {?} */ eta = null;
            xhr.upload.addEventListener('progress', function (e) {
                if (e.lengthComputable) {
                    var /** @type {?} */ percentage = Math.round((e.loaded * 100) / e.total);
                    var /** @type {?} */ diff = new Date().getTime() - time;
                    speed = Math.round(e.loaded / diff * 1000);
                    progressStartTime = (file.progress.data && file.progress.data.startTime) || new Date().getTime();
                    eta = Math.ceil((e.total - e.loaded) / speed);
                    file.progress = {
                        status: UploadStatus.Uploading,
                        data: {
                            percentage: percentage,
                            speed: speed,
                            speedHuman: humanizeBytes(speed) + "/s",
                            startTime: progressStartTime,
                            endTime: null,
                            eta: eta,
                            etaHuman: _this.secondsToHuman(eta)
                        }
                    };
                    observer.next({ type: 'uploading', file: file });
                }
            }, false);
            xhr.upload.addEventListener('error', function (e) {
                observer.error(e);
                observer.complete();
            });
            xhr.onreadystatechange = function () {
                if (xhr.readyState === XMLHttpRequest.DONE) {
                    var /** @type {?} */ speedAverage = Math.round(file.size / (new Date().getTime() - progressStartTime) * 1000);
                    file.progress = {
                        status: UploadStatus.Done,
                        data: {
                            percentage: 100,
                            speed: speedAverage,
                            speedHuman: humanizeBytes(speedAverage) + "/s",
                            startTime: progressStartTime,
                            endTime: new Date().getTime(),
                            eta: eta,
                            etaHuman: _this.secondsToHuman(eta || 0)
                        }
                    };
                    file.responseStatus = xhr.status;
                    try {
                        file.response = JSON.parse(xhr.response);
                    }
                    catch (e) {
                        file.response = xhr.response;
                    }
                    file.responseHeaders = _this.parseResponseHeaders(xhr.getAllResponseHeaders());
                    observer.next({ type: 'done', file: file });
                    observer.complete();
                }
            };
            xhr.open(method, url, true);
            xhr.withCredentials = event.withCredentials ? true : false;
            try {
                var /** @type {?} */ uploadFile_1 = (file.nativeFile);
                var /** @type {?} */ uploadIndex = _this.queue.findIndex(function (outFile) { return outFile.nativeFile === uploadFile_1; });
                if (_this.queue[uploadIndex].progress.status === UploadStatus.Cancelled) {
                    observer.complete();
                }
                file.form.append(event.fieldName || 'file', uploadFile_1, uploadFile_1.name);
                Object.keys(data).forEach(function (key) { return file.form.append(key, data[key]); });
                Object.keys(headers).forEach(function (key) { return xhr.setRequestHeader(key, headers[key]); });
                _this.serviceEvents.emit({ type: 'start', file: file });
                xhr.send(file.form);
            }
            catch (e) {
                observer.complete();
            }
            return function () {
                xhr.abort();
            };
        });
    };
    /**
     * @param {?} sec
     * @return {?}
     */
    NgUploaderService.prototype.secondsToHuman = function (sec) {
        return new Date(sec * 1000).toISOString().substr(11, 8);
    };
    /**
     * @return {?}
     */
    NgUploaderService.prototype.generateId = function () {
        return Math.random().toString(36).substring(7);
    };
    /**
     * @param {?} contentTypes
     * @return {?}
     */
    NgUploaderService.prototype.setContentTypes = function (contentTypes) {
        if (typeof contentTypes != 'undefined' && contentTypes instanceof Array) {
            if (contentTypes.find(function (type) { return type === '*'; }) !== undefined) {
                this.contentTypes = ['*'];
            }
            else {
                this.contentTypes = contentTypes;
            }
            return;
        }
        this.contentTypes = ['*'];
    };
    /**
     * @return {?}
     */
    NgUploaderService.prototype.allContentTypesAllowed = function () {
        return this.contentTypes.find(function (type) { return type === '*'; }) !== undefined;
    };
    /**
     * @param {?} mimetype
     * @return {?}
     */
    NgUploaderService.prototype.isContentTypeAllowed = function (mimetype) {
        if (this.allContentTypesAllowed()) {
            return true;
        }
        return this.contentTypes.find(function (type) { return type === mimetype; }) !== undefined;
    };
    /**
     * @param {?} file
     * @param {?} index
     * @return {?}
     */
    NgUploaderService.prototype.makeUploadFile = function (file, index) {
        return {
            fileIndex: index,
            id: this.generateId(),
            name: file.name,
            size: file.size,
            type: file.type,
            form: new FormData(),
            progress: {
                status: UploadStatus.Queue,
                data: {
                    percentage: 0,
                    speed: 0,
                    speedHuman: humanizeBytes(0) + "/s",
                    startTime: null,
                    endTime: null,
                    eta: null,
                    etaHuman: null
                }
            },
            lastModifiedDate: file.lastModifiedDate,
            sub: undefined,
            nativeFile: file
        };
    };
    /**
     * @param {?} httpHeaders
     * @return {?}
     */
    NgUploaderService.prototype.parseResponseHeaders = function (httpHeaders) {
        if (!httpHeaders) {
            return;
        }
        return httpHeaders.split('\n')
            .map(function (x) { return x.split(/: */, 2); })
            .filter(function (x) { return x[0]; })
            .reduce(function (ac, x) {
            ac[x[0]] = x[1];
            return ac;
        }, {});
    };
    return NgUploaderService;
}());
var NgFileSelectDirective = /** @class */ (function () {
    /**
     * @param {?} elementRef
     */
    function NgFileSelectDirective(elementRef) {
        var _this = this;
        this.elementRef = elementRef;
        this.fileListener = function () {
            if (_this.el.files) {
                _this.upload.handleFiles(_this.el.files);
            }
        };
        this.uploadOutput = new EventEmitter();
    }
    /**
     * @return {?}
     */
    NgFileSelectDirective.prototype.ngOnInit = function () {
        var _this = this;
        this._sub = [];
        var /** @type {?} */ concurrency = this.options && this.options.concurrency || Number.POSITIVE_INFINITY;
        var /** @type {?} */ allowedContentTypes = this.options && this.options.allowedContentTypes || ['*'];
        this.upload = new NgUploaderService(concurrency, allowedContentTypes);
        this.el = this.elementRef.nativeElement;
        this.el.addEventListener('change', this.fileListener, false);
        this._sub.push(this.upload.serviceEvents.subscribe(function (event) {
            _this.uploadOutput.emit(event);
        }));
        if (this.uploadInput instanceof EventEmitter) {
            this._sub.push(this.upload.initInputEvents(this.uploadInput));
        }
    };
    /**
     * @return {?}
     */
    NgFileSelectDirective.prototype.ngOnDestroy = function () {
        this.el.removeEventListener('change', this.fileListener, false);
        this._sub.forEach(function (sub) { return sub.unsubscribe(); });
    };
    return NgFileSelectDirective;
}());
NgFileSelectDirective.decorators = [
    { type: Directive, args: [{
                selector: '[ngFileSelect]'
            },] },
];
/**
 * @nocollapse
 */
NgFileSelectDirective.ctorParameters = function () { return [
    { type: ElementRef, },
]; };
NgFileSelectDirective.propDecorators = {
    'options': [{ type: Input },],
    'uploadInput': [{ type: Input },],
    'uploadOutput': [{ type: Output },],
};
var NgFileDropDirective = /** @class */ (function () {
    /**
     * @param {?} elementRef
     */
    function NgFileDropDirective(elementRef) {
        this.elementRef = elementRef;
        this.stopEvent = function (e) {
            e.stopPropagation();
            e.preventDefault();
        };
        this.uploadOutput = new EventEmitter();
    }
    /**
     * @return {?}
     */
    NgFileDropDirective.prototype.ngOnInit = function () {
        var _this = this;
        this._sub = [];
        var /** @type {?} */ concurrency = this.options && this.options.concurrency || Number.POSITIVE_INFINITY;
        var /** @type {?} */ allowedContentTypes = this.options && this.options.allowedContentTypes || ['*'];
        this.upload = new NgUploaderService(concurrency, allowedContentTypes);
        this.el = this.elementRef.nativeElement;
        this._sub.push(this.upload.serviceEvents.subscribe(function (event) {
            _this.uploadOutput.emit(event);
        }));
        if (this.uploadInput instanceof EventEmitter) {
            this._sub.push(this.upload.initInputEvents(this.uploadInput));
        }
        this.el.addEventListener('drop', this.stopEvent, false);
        this.el.addEventListener('dragenter', this.stopEvent, false);
        this.el.addEventListener('dragover', this.stopEvent, false);
    };
    /**
     * @return {?}
     */
    NgFileDropDirective.prototype.ngOnDestroy = function () {
        this._sub.forEach(function (sub) { return sub.unsubscribe(); });
    };
    /**
     * @param {?} e
     * @return {?}
     */
    NgFileDropDirective.prototype.onDrop = function (e) {
        e.stopPropagation();
        e.preventDefault();
        var /** @type {?} */ event = { type: 'drop' };
        this.uploadOutput.emit(event);
        this.upload.handleFiles(e.dataTransfer.files);
    };
    /**
     * @param {?} e
     * @return {?}
     */
    NgFileDropDirective.prototype.onDragOver = function (e) {
        if (!e) {
            return;
        }
        var /** @type {?} */ event = { type: 'dragOver' };
        this.uploadOutput.emit(event);
    };
    /**
     * @param {?} e
     * @return {?}
     */
    NgFileDropDirective.prototype.onDragLeave = function (e) {
        if (!e) {
            return;
        }
        var /** @type {?} */ event = { type: 'dragOut' };
        this.uploadOutput.emit(event);
    };
    return NgFileDropDirective;
}());
NgFileDropDirective.decorators = [
    { type: Directive, args: [{
                selector: '[ngFileDrop]'
            },] },
];
/**
 * @nocollapse
 */
NgFileDropDirective.ctorParameters = function () { return [
    { type: ElementRef, },
]; };
NgFileDropDirective.propDecorators = {
    'options': [{ type: Input },],
    'uploadInput': [{ type: Input },],
    'uploadOutput': [{ type: Output },],
    'onDrop': [{ type: HostListener, args: ['drop', ['$event'],] },],
    'onDragOver': [{ type: HostListener, args: ['dragover', ['$event'],] },],
    'onDragLeave': [{ type: HostListener, args: ['dragleave', ['$event'],] },],
};
var NgUploaderModule = /** @class */ (function () {
    function NgUploaderModule() {
    }
    return NgUploaderModule;
}());
NgUploaderModule.decorators = [
    { type: NgModule, args: [{
                declarations: [
                    NgFileSelectDirective,
                    NgFileDropDirective
                ],
                exports: [
                    NgFileSelectDirective,
                    NgFileDropDirective
                ]
            },] },
];
/**
 * @nocollapse
 */
NgUploaderModule.ctorParameters = function () { return []; };
/**
 * Generated bundle index. Do not edit.
 */
export { NgUploaderModule, humanizeBytes, NgUploaderService, UploadStatus, NgFileSelectDirective, NgFileDropDirective };
//# sourceMappingURL=ngx-uploader.es5.js.map
