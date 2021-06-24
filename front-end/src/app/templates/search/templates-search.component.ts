import { Component, OnInit, ViewEncapsulation } from '@angular/core';
import { ActivityService, AlertUtil, CallService, CommonUtil, IntentService, ManagerService, _TemplateComponent } from '@mbs/widgets';
import Swal from 'sweetalert2';

declare const window : any;

@Component({
  selector: 'mbs-templates-search',
  templateUrl: './templates-search.component.html',
  styleUrls: ['./templates-search.component.scss'],
  encapsulation: ViewEncapsulation.None,
})
export class TemplatesSearchComponent extends _TemplateComponent implements OnInit {

  constructor(
    protected activityService: ActivityService,
    protected callService: CallService,
    protected intentService: IntentService,
    protected managerService: ManagerService,
  ) {
    super(activityService, callService, intentService, managerService);
  }
  
  ngOnInit(): void {
    console.log('search.ngOnInit.this.rootHtmlElement', this.rootHtmlElement)
    this.onSearchInit();
  }

  async searchData(changeTableResult?: any): Promise<void> {
    const dataForSearch = CommonUtil.copy(this.template.page.search.criteria);
    if (this.validateEmptySearch(dataForSearch)) {
      Swal.fire({
        title: 'ไม่สามารถค้นหาได้',
        text: 'โปรดกรอกข้อมูลอย่างน้อย 1 เงื่อนไข',
        icon: 'warning',
        confirmButtonColor: '#58748B',
        confirmButtonText: 'ตกลง',
      });
    } else {
      this.processConcatContainField(dataForSearch);
      dataForSearch.NOT_80005 = 6;
      const defaultPagination = this.getDefaultPagination();
      const param = {
        data: {
          fields: dataForSearch,
          sorting: changeTableResult ? changeTableResult.sort : null,
          options: {
            start: changeTableResult ? this.template.page.search.table.pagination.start : defaultPagination.start,
            limit: changeTableResult ? this.template.page.search.table.pagination.limit : defaultPagination.limit,
            embed_public: true,
            period: 1,
          },
          need_verify: false,
        },
        meta: {},
      };
      try {
        const result = await this.activityService.search(param);
        if (result) {
          if (result.meta.response_code === '10000') {
            if (result.data.response_data.length == 1 && this.template.page.search.isAutoRedirect && this.template.page.search.table.pagination.start === 1 && this.isEnableEdit(result.data.response_data[0])) {
              this.goToView(result.data.response_data[0]);
            }
            this.template.page.search.table.sources = result.data.response_data;
            this.template.page.search.table.pagination.total = result.data.hits;
          } else if (result.meta.response_code === '32401') {
            Swal.fire({
              title: 'ไม่พบข้อมูล',
              text: '',
              icon: 'warning',
              confirmButtonText: 'ตกลง',
              confirmButtonColor: '#58748B',
            });
          } else {
            AlertUtil.fireError({meta: result.meta});
          }
        } else {
          AlertUtil.fireError();
        }
      } catch (error) {
        AlertUtil.fireError();
      }
    }
  }

  isEnableEdit = (row: any): boolean => {
    let isEnable = true;
    return isEnable;
  }
  
}
