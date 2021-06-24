import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { MbsElementComponent } from './mbs-element.component';

describe('MbsElementComponent', () => {
  let component: MbsElementComponent;
  let fixture: ComponentFixture<MbsElementComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ MbsElementComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(MbsElementComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
