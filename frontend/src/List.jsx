import React, { Component } from 'react';
import 'whatwg-fetch';

export default class List extends Component {
    // initial state
    state = {
        list: []
    };

    componentWillMount(){
        fetch('https://jsonplaceholder.typicode.com/posts')
            .then(response => response.json())
            .then(json => this.setState({
                list: json
            }));
    }

    render() {
        return (
            <div>
                <ul>
                    {this.state.list.map((item) => {
                        return <li key={item.id}>{item.title}</li>;
                    })}
                </ul>
            </div>
        );
    }
}