import { Component, OnInit, ViewEncapsulation } from '@angular/core';
import { ActivityService, CallService, IntentService, ManagerService, _TemplateComponent } from '@mbs/widgets';

declare const window : any;

@Component({
  selector: 'mbs-templates-view',
  templateUrl: './templates-view.component.html',
  styleUrls: ['./templates-view.component.scss'],
  encapsulation: ViewEncapsulation.None,
})
export class TemplatesViewComponent extends _TemplateComponent implements OnInit {

  constructor(
    protected activityService: ActivityService,
    protected callService: CallService,
    protected intentService: IntentService,
    protected managerService: ManagerService,
  ) {
    super(activityService, callService, intentService, managerService);
  }
  
  ngOnInit(): void {
    this.onViewInit();
  }

}
