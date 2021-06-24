import { APP_BASE_HREF, CommonModule } from '@angular/common';
import { HttpClientModule, HTTP_INTERCEPTORS } from '@angular/common/http';
import { Injector, NgModule, CUSTOM_ELEMENTS_SCHEMA, ErrorHandler } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { BrowserModule } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { CollapseModule } from 'ngx-bootstrap/collapse';
import { AppRoutingModule } from './app-routing.module';
import { AppComponent } from './app.component';
import { MbsElementComponent } from './mbs-element/mbs-element.component';
import { TemplatesMainComponent } from './templates/main/templates-main.component';
import { TemplatesSearchComponent } from './templates/search/templates-search.component';
import { TemplatesViewComponent } from './templates/view/templates-view.component';
import { WidgetButtonModule, WidgetCameraUploadModule, WidgetCheckboxModule, WidgetDateBetweenModule, WidgetDatetimePickerModule, WidgetDisplayModule, WidgetDropdownModule, WidgetFilesDisplayModule, WidgetIconButtonModule, WidgetMapsModule, WidgetMasterDatasetDisplayModule, WidgetMasterDatasetModule, WidgetModalModule, WidgetMultiCheckboxModule, WidgetNumberModule, WidgetPanelModule, WidgetRadioButtonModule, WidgetRankingModule, WidgetRatingModule, WidgetTableModule, WidgetTextAreaModule, WidgetTextDisplayModule, WidgetTextboxModule, WidgetToggleModule, WidgetUploaderModule, WidgetRichTextModule, WidgetSignatureModule, WidgetPreformattedTextModule, WidgetMultiSelectModule, WidgetChipsModule, ActivityService, CallService, IntentService, ManagerService, LoggingService, CommonSearchModule, CommonCreateModule, CommonUpdateModule, CommonViewModule, CommonFieldModule, RequestInterceptor } from '@mbs/widgets';
import { createCustomElement } from  '@angular/elements'
import { AngularEditorModule } from '@kolkov/angular-editor';

const widgetModules = [
  WidgetButtonModule, WidgetCameraUploadModule, WidgetCheckboxModule, WidgetDateBetweenModule,
  WidgetDatetimePickerModule, WidgetDisplayModule, WidgetDropdownModule, WidgetFilesDisplayModule,
  WidgetIconButtonModule, WidgetMapsModule, WidgetMasterDatasetDisplayModule, WidgetMasterDatasetModule,
  WidgetModalModule, WidgetMultiCheckboxModule, WidgetNumberModule, WidgetPanelModule, WidgetRadioButtonModule,
  WidgetRankingModule, WidgetRatingModule, WidgetTableModule, WidgetTextAreaModule, WidgetTextDisplayModule,
  WidgetTextboxModule, WidgetToggleModule, WidgetUploaderModule, WidgetRichTextModule, WidgetSignatureModule,
  WidgetPreformattedTextModule, WidgetMultiSelectModule, WidgetChipsModule, CommonSearchModule, CommonCreateModule, CommonUpdateModule, CommonViewModule, CommonFieldModule
];

const CommonService = [
  ActivityService, CallService, IntentService, ManagerService
]

@NgModule({
  declarations: [
    AppComponent,
    MbsElementComponent,
    TemplatesMainComponent,
    TemplatesSearchComponent,
    TemplatesViewComponent,
  ],
  imports: [
    CommonModule,
    BrowserModule,
    FormsModule,
    BrowserAnimationsModule,
    AppRoutingModule,
    HttpClientModule,
    AngularEditorModule,
    ...widgetModules,
    CollapseModule.forRoot()
  ],
  entryComponents: [MbsElementComponent,
    TemplatesMainComponent,
    TemplatesSearchComponent,
    TemplatesViewComponent,
  ],
  providers: [
    {
      provide: ErrorHandler,
      useClass: LoggingService,
    },
    {
      provide: HTTP_INTERCEPTORS, 
      useClass: RequestInterceptor, 
      multi: true
    },
    {provide: APP_BASE_HREF, useValue: '/'},
    ...CommonService,
  ],
  schemas: [CUSTOM_ELEMENTS_SCHEMA],
  bootstrap: [AppComponent]
})
export class AppModule {

  constructor(private injector: Injector) { }

  ngDoBootstrap() {
    //Declares our component's Custom Element
    // Then defines it in the DOM so it can be used in other projects
    // const customElement = createCustomElement(MbsElementComponent, { injector: this.injector });
    // customElements.define('mbs-mbs-element.component', customElement);

    // const customElement2 = createCustomElement(TemplatesI9IdentityNode2Component, { injector: this.injector });
    // customElements.define('mbs-templates-i9-identity-2', customElement2);

    // const customElement3 = createCustomElement(TemplatesI9RelationshipComponent, { injector: this.injector });
    // customElements.define('mbs-templates-i9-relationship-1', customElement3);

    // const customElement4 = createCustomElement(TemplatesI9RelationshipNode2Component, { injector: this.injector });
    // customElements.define('mbs-templates-i9-relationship-2', customElement4);

    // const customElement5 = createCustomElement(TemplatesI9PhysicalContactComponent, { injector: this.injector });
    // customElements.define('mbs-templates-i9-physical-contact-1', customElement5);

    // const customElement6 = createCustomElement(TemplatesI9PhysicalContactNode2Component, { injector: this.injector });
    // customElements.define('mbs-templates-i9-physical-contact-2', customElement6);

    // const customElement7 = createCustomElement(TemplatesO2PhysicalContactComponent, { injector: this.injector });
    // customElements.define('mbs-templates-o2-physical-contact-1', customElement7);

    // const customElement8 = createCustomElement(TemplatesO2PhysicalContactNode2Component, { injector: this.injector });
    // customElements.define('mbs-templates-o2-physical-contact-2', customElement8);

    //const customElement8 = createCustomElement(MbsElementComponent, { injector: this.injector });
    //customElements.define('mbs-3567728f-2236-43bd-b9de-c97fd32f3649', customElement8);


  }
}
