import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { TemplatesSearchComponent } from './templates-search.component';

describe('TemplatesI9SearchComponent', () => {
  let component: TemplatesSearchComponent;
  let fixture: ComponentFixture<TemplatesSearchComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ TemplatesSearchComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(TemplatesSearchComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
