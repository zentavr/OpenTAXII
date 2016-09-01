import React, { Component } from 'react';
import List from './List';

export default class App extends Component {
    render() {
        return (
            <div>
                <h1>OpenTAXII</h1>
                <List />
            </div>
        );
    }
}