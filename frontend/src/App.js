import 'whatwg-fetch';

import React, { Component } from 'react';
import ResourceList from './List';

require("./assets/main.css")


export default class App extends Component {
    render() {
        return (
            <div>
                <h2>OpenTAXII hey!</h2>
                <ResourceList resource_type={'services'} />
                <ResourceList resource_type={'collections'} />
                <ResourceList resource_type={'content-blocks'} />
            </div>
        );
    }
}
