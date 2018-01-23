import { Directive, ElementRef, EventEmitter, HostListener, Input, NgModule, Output } from '@angular/core';
import { Observable as Observable$1 } from 'rxjs/Observable';
import { Subject as Subject$1 } from 'rxjs/Subject';
import 'rxjs/add/operator/mergeMap';

let UploadStatus = {};
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
    const /** @type {?} */ k = 1024;
    const /** @type {?} */ sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB'];
    const /** @type {?} */ i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}
class NgUploaderService {
    /**
     * @param {?=} concurrency
     * @param {?=} contentTypes
     */
    constructor(concurrency = Number.POSITIVE_INFINITY, contentTypes = ['*']) {
        this.queue = [];
        this.serviceEvents = new EventEmitter();
        this.uploadScheduler = new Subject$1();
        this.subs = [];
        this.contentTypes = contentTypes;
        this.uploadScheduler
            .mergeMap(upload => this.startUpload(upload), concurrency)
            .subscribe(uploadOutput => this.serviceEvents.emit(uploadOutput));
    }
    /**
     * @param {?} incomingFiles
     * @return {?}
     */
    handleFiles(incomingFiles) {
        const /** @type {?} */ allowedIncomingFiles = [].reduce.call(incomingFiles, (acc, checkFile, i) => {
            if (this.isContentTypeAllowed(checkFile.type)) {
                acc = acc.concat(checkFile);
            }
            else {
                const /** @type {?} */ rejectedFile = this.makeUploadFile(checkFile, i);
                this.serviceEvents.emit({ type: 'rejected', file: rejectedFile });
            }
            return acc;
        }, []);
        this.queue.push(...[].map.call(allowedIncomingFiles, (file, i) => {
            const /** @type {?} */ uploadFile = this.makeUploadFile(file, i);
            this.serviceEvents.emit({ type: 'addedToQueue', file: uploadFile });
            return uploadFile;
        }));
        this.serviceEvents.emit({ type: 'allAddedToQueue' });
    }
    /**
     * @param {?} input
     * @return {?}
     */
    initInputEvents(input) {
        return input.subscribe((event) => {
            switch (event.type) {
                case 'uploadFile':
                    const /** @type {?} */ uploadFileIndex = this.queue.findIndex(file => file === event.file);
                    if (uploadFileIndex !== -1 && event.file) {
                        this.uploadScheduler.next({ file: this.queue[uploadFileIndex], event: event });
                    }
                    break;
                case 'uploadAll':
                    const /** @type {?} */ files = this.queue.filter(file => file.progress.status === UploadStatus.Queue);
                    files.forEach(file => this.uploadScheduler.next({ file: file, event: event }));
                    break;
                case 'cancel':
                    const /** @type {?} */ id = event.id || null;
                    if (!id) {
                        return;
                    }
                    const /** @type {?} */ index = this.subs.findIndex(sub => sub.id === id);
                    if (index !== -1 && this.subs[index].sub) {
                        this.subs[index].sub.unsubscribe();
                        const /** @type {?} */ fileIndex = this.queue.findIndex(file => file.id === id);
                        if (fileIndex !== -1) {
                            this.queue[fileIndex].progress.status = UploadStatus.Cancelled;
                            this.serviceEvents.emit({ type: 'cancelled', file: this.queue[fileIndex] });
                        }
                    }
                    break;
                case 'cancelAll':
                    this.subs.forEach(sub => {
                        if (sub.sub) {
                            sub.sub.unsubscribe();
                        }
                        const /** @type {?} */ file = this.queue.find(uploadFile => uploadFile.id === sub.id);
                        if (file) {
                            file.progress.status = UploadStatus.Cancelled;
                            this.serviceEvents.emit({ type: 'cancelled', file: file });
                        }
                    });
                    break;
                case 'remove':
                    if (!event.id) {
                        return;
                    }
                    const /** @type {?} */ i = this.queue.findIndex(file => file.id === event.id);
                    if (i !== -1) {
                        const /** @type {?} */ file = this.queue[i];
                        this.queue.splice(i, 1);
                        this.serviceEvents.emit({ type: 'removed', file: file });
                    }
                    break;
                case 'removeAll':
                    if (this.queue.length) {
                        this.queue = [];
                        this.serviceEvents.emit({ type: 'removedAll' });
                    }
                    break;
            }
        });
    }
    /**
     * @param {?} upload
     * @return {?}
     */
    startUpload(upload) {
        return new Observable$1(observer => {
            const /** @type {?} */ sub = this.uploadFile(upload.file, upload.event)
                .subscribe(output => {
                observer.next(output);
            }, err => {
                observer.error(err);
                observer.complete();
            }, () => {
                observer.complete();
            });
            this.subs.push({ id: upload.file.id, sub: sub });
        });
    }
    /**
     * @param {?} file
     * @param {?} event
     * @return {?}
     */
    uploadFile(file, event) {
        return new Observable$1(observer => {
            const /** @type {?} */ url = event.url || '';
            const /** @type {?} */ method = event.method || 'POST';
            const /** @type {?} */ data = event.data || {};
            const /** @type {?} */ headers = event.headers || {};
            const /** @type {?} */ xhr = new XMLHttpRequest();
            const /** @type {?} */ time = new Date().getTime();
            let /** @type {?} */ progressStartTime = (file.progress.data && file.progress.data.startTime) || time;
            let /** @type {?} */ speed = 0;
            let /** @type {?} */ eta = null;
            xhr.upload.addEventListener('progress', (e) => {
                if (e.lengthComputable) {
                    const /** @type {?} */ percentage = Math.round((e.loaded * 100) / e.total);
                    const /** @type {?} */ diff = new Date().getTime() - time;
                    speed = Math.round(e.loaded / diff * 1000);
                    progressStartTime = (file.progress.data && file.progress.data.startTime) || new Date().getTime();
                    eta = Math.ceil((e.total - e.loaded) / speed);
                    file.progress = {
                        status: UploadStatus.Uploading,
                        data: {
                            percentage: percentage,
                            speed: speed,
                            speedHuman: `${humanizeBytes(speed)}/s`,
                            startTime: progressStartTime,
                            endTime: null,
                            eta: eta,
                            etaHuman: this.secondsToHuman(eta)
                        }
                    };
                    observer.next({ type: 'uploading', file: file });
                }
            }, false);
            xhr.upload.addEventListener('error', (e) => {
                observer.error(e);
                observer.complete();
            });
            xhr.onreadystatechange = () => {
                if (xhr.readyState === XMLHttpRequest.DONE) {
                    const /** @type {?} */ speedAverage = Math.round(file.size / (new Date().getTime() - progressStartTime) * 1000);
                    file.progress = {
                        status: UploadStatus.Done,
                        data: {
                            percentage: 100,
                            speed: speedAverage,
                            speedHuman: `${humanizeBytes(speedAverage)}/s`,
                            startTime: progressStartTime,
                            endTime: new Date().getTime(),
                            eta: eta,
                            etaHuman: this.secondsToHuman(eta || 0)
                        }
                    };
                    file.responseStatus = xhr.status;
                    try {
                        file.response = JSON.parse(xhr.response);
                    }
                    catch (e) {
                        file.response = xhr.response;
                    }
                    file.responseHeaders = this.parseResponseHeaders(xhr.getAllResponseHeaders());
                    observer.next({ type: 'done', file: file });
                    observer.complete();
                }
            };
            xhr.open(method, url, true);
            xhr.withCredentials = event.withCredentials ? true : false;
            try {
                const /** @type {?} */ uploadFile = (file.nativeFile);
                const /** @type {?} */ uploadIndex = this.queue.findIndex(outFile => outFile.nativeFile === uploadFile);
                if (this.queue[uploadIndex].progress.status === UploadStatus.Cancelled) {
                    observer.complete();
                }
                file.form.append(event.fieldName || 'file', uploadFile, uploadFile.name);
                Object.keys(data).forEach(key => file.form.append(key, data[key]));
                Object.keys(headers).forEach(key => xhr.setRequestHeader(key, headers[key]));
                this.serviceEvents.emit({ type: 'start', file: file });
                xhr.send(file.form);
            }
            catch (e) {
                observer.complete();
            }
            return () => {
                xhr.abort();
            };
        });
    }
    /**
     * @param {?} sec
     * @return {?}
     */
    secondsToHuman(sec) {
        return new Date(sec * 1000).toISOString().substr(11, 8);
    }
    /**
     * @return {?}
     */
    generateId() {
        return Math.random().toString(36).substring(7);
    }
    /**
     * @param {?} contentTypes
     * @return {?}
     */
    setContentTypes(contentTypes) {
        if (typeof contentTypes != 'undefined' && contentTypes instanceof Array) {
            if (contentTypes.find((type) => type === '*') !== undefined) {
                this.contentTypes = ['*'];
            }
            else {
                this.contentTypes = contentTypes;
            }
            return;
        }
        this.contentTypes = ['*'];
    }
    /**
     * @return {?}
     */
    allContentTypesAllowed() {
        return this.contentTypes.find((type) => type === '*') !== undefined;
    }
    /**
     * @param {?} mimetype
     * @return {?}
     */
    isContentTypeAllowed(mimetype) {
        if (this.allContentTypesAllowed()) {
            return true;
        }
        return this.contentTypes.find((type) => type === mimetype) !== undefined;
    }
    /**
     * @param {?} file
     * @param {?} index
     * @return {?}
     */
    makeUploadFile(file, index) {
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
                    speedHuman: `${humanizeBytes(0)}/s`,
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
    }
    /**
     * @param {?} httpHeaders
     * @return {?}
     */
    parseResponseHeaders(httpHeaders) {
        if (!httpHeaders) {
            return;
        }
        return httpHeaders.split('\n')
            .map(x => x.split(/: */, 2))
            .filter(x => x[0])
            .reduce((ac, x) => {
            ac[x[0]] = x[1];
            return ac;
        }, {});
    }
}

class NgFileSelectDirective {
    /**
     * @param {?} elementRef
     */
    constructor(elementRef) {
        this.elementRef = elementRef;
        this.fileListener = () => {
            if (this.el.files) {
                this.upload.handleFiles(this.el.files);
            }
        };
        this.uploadOutput = new EventEmitter();
    }
    /**
     * @return {?}
     */
    ngOnInit() {
        this._sub = [];
        const /** @type {?} */ concurrency = this.options && this.options.concurrency || Number.POSITIVE_INFINITY;
        const /** @type {?} */ allowedContentTypes = this.options && this.options.allowedContentTypes || ['*'];
        this.upload = new NgUploaderService(concurrency, allowedContentTypes);
        this.el = this.elementRef.nativeElement;
        this.el.addEventListener('change', this.fileListener, false);
        this._sub.push(this.upload.serviceEvents.subscribe((event) => {
            this.uploadOutput.emit(event);
        }));
        if (this.uploadInput instanceof EventEmitter) {
            this._sub.push(this.upload.initInputEvents(this.uploadInput));
        }
    }
    /**
     * @return {?}
     */
    ngOnDestroy() {
        this.el.removeEventListener('change', this.fileListener, false);
        this._sub.forEach(sub => sub.unsubscribe());
    }
}
NgFileSelectDirective.decorators = [
    { type: Directive, args: [{
                selector: '[ngFileSelect]'
            },] },
];
/**
 * @nocollapse
 */
NgFileSelectDirective.ctorParameters = () => [
    { type: ElementRef, },
];
NgFileSelectDirective.propDecorators = {
    'options': [{ type: Input },],
    'uploadInput': [{ type: Input },],
    'uploadOutput': [{ type: Output },],
};

class NgFileDropDirective {
    /**
     * @param {?} elementRef
     */
    constructor(elementRef) {
        this.elementRef = elementRef;
        this.stopEvent = (e) => {
            e.stopPropagation();
            e.preventDefault();
        };
        this.uploadOutput = new EventEmitter();
    }
    /**
     * @return {?}
     */
    ngOnInit() {
        this._sub = [];
        const /** @type {?} */ concurrency = this.options && this.options.concurrency || Number.POSITIVE_INFINITY;
        const /** @type {?} */ allowedContentTypes = this.options && this.options.allowedContentTypes || ['*'];
        this.upload = new NgUploaderService(concurrency, allowedContentTypes);
        this.el = this.elementRef.nativeElement;
        this._sub.push(this.upload.serviceEvents.subscribe((event) => {
            this.uploadOutput.emit(event);
        }));
        if (this.uploadInput instanceof EventEmitter) {
            this._sub.push(this.upload.initInputEvents(this.uploadInput));
        }
        this.el.addEventListener('drop', this.stopEvent, false);
        this.el.addEventListener('dragenter', this.stopEvent, false);
        this.el.addEventListener('dragover', this.stopEvent, false);
    }
    /**
     * @return {?}
     */
    ngOnDestroy() {
        this._sub.forEach(sub => sub.unsubscribe());
    }
    /**
     * @param {?} e
     * @return {?}
     */
    onDrop(e) {
        e.stopPropagation();
        e.preventDefault();
        const /** @type {?} */ event = { type: 'drop' };
        this.uploadOutput.emit(event);
        this.upload.handleFiles(e.dataTransfer.files);
    }
    /**
     * @param {?} e
     * @return {?}
     */
    onDragOver(e) {
        if (!e) {
            return;
        }
        const /** @type {?} */ event = { type: 'dragOver' };
        this.uploadOutput.emit(event);
    }
    /**
     * @param {?} e
     * @return {?}
     */
    onDragLeave(e) {
        if (!e) {
            return;
        }
        const /** @type {?} */ event = { type: 'dragOut' };
        this.uploadOutput.emit(event);
    }
}
NgFileDropDirective.decorators = [
    { type: Directive, args: [{
                selector: '[ngFileDrop]'
            },] },
];
/**
 * @nocollapse
 */
NgFileDropDirective.ctorParameters = () => [
    { type: ElementRef, },
];
NgFileDropDirective.propDecorators = {
    'options': [{ type: Input },],
    'uploadInput': [{ type: Input },],
    'uploadOutput': [{ type: Output },],
    'onDrop': [{ type: HostListener, args: ['drop', ['$event'],] },],
    'onDragOver': [{ type: HostListener, args: ['dragover', ['$event'],] },],
    'onDragLeave': [{ type: HostListener, args: ['dragleave', ['$event'],] },],
};

class NgUploaderModule {
}
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
NgUploaderModule.ctorParameters = () => [];

/**
 * Generated bundle index. Do not edit.
 */

export { NgUploaderModule, humanizeBytes, NgUploaderService, UploadStatus, NgFileSelectDirective, NgFileDropDirective };
//# sourceMappingURL=ngx-uploader.js.map
