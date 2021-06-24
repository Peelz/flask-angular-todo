import { HttpClient } from '@angular/common/http';
import { Component, ElementRef, Input, OnInit, ViewEncapsulation } from '@angular/core';
import { ActivityService, CallService, IntentService, ManagerService, _TemplateComponent } from '@mbs/widgets';

@Component({
    selector: 'mbs-mbs-element',
    templateUrl: './mbs-element.component.html',
    styleUrls: ['./mbs-element.component.scss'],
    encapsulation: ViewEncapsulation.None,
})
export class MbsElementComponent extends _TemplateComponent implements OnInit {

    @Input() httpClient: HttpClient;

    constructor(
        protected activityService: ActivityService,
        protected callService: CallService,
        protected intentService: IntentService,
        protected managerService: ManagerService,
        protected hostElement: ElementRef,
    ) {
        super(activityService, callService, intentService, managerService);
    }

    ngOnInit(): void {
        this.rootHtmlElement = (this.hostElement.nativeElement as HTMLElement);
        console.log('element.ngOnInit.this.httpClient', this.httpClient);
        console.log('element.ngOnInit.this.loaderService', this.loaderService);
        console.log('element.constructor.this.rootHtmlElement', this.rootHtmlElement)
    }

}
