import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { TemplatesMainComponent } from './templates-main.component';

describe('TemplatesI9MainComponent', () => {
  let component: TemplatesMainComponent;
  let fixture: ComponentFixture<TemplatesMainComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ TemplatesMainComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(TemplatesMainComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
