import { Component } from '@angular/core';

declare const window : any;

@Component({
    selector: 'app-root',
    templateUrl: './app.component.html',
    styleUrls: ['./app.component.scss']
})
export class AppComponent {

    constructor() {
        window.is_run_local = false;
    };

}
