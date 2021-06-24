import { Component, OnDestroy, OnInit, ViewEncapsulation } from '@angular/core';
import { ActivityService, CallService, IntentService, ManagerService, _TemplateComponent } from '@mbs/widgets';

declare const window : any;

@Component({
  selector: 'mbs-templates-main',
  templateUrl: './templates-main.component.html',
  styleUrls: ['./templates-main.component.scss'],
  encapsulation: ViewEncapsulation.None
})
export class TemplatesMainComponent extends _TemplateComponent implements OnInit, OnDestroy {

  constructor(
    protected activityService: ActivityService,
    protected callService: CallService,
    protected intentService: IntentService,
    protected managerService: ManagerService,
  ) {
    super(activityService, callService, intentService, managerService);
  }

  ngOnInit() {
    console.log('main.ngOnInit.this.launcherHttpClient', this.launcherHttpClient)
    console.log('main.ngOnInit.this.rootHtmlElement', this.rootHtmlElement)
    this.onMainInit();
  }

  ngOnDestroy(): void {
    this.onMainDestroy();
  }

}
