import React, { Component } from 'react';
import 'whatwg-fetch';

export default class List extends Component {
    // initial state
    state = {
        num: 0
    };

    componentWillMount(){
        fetch('https://jsonplaceholder.typicode.com/posts')
            .then(response => response.json())
            .then(json => this.setState({
                list: json
            }));
    }

    _plusOne(){
        this.setState({
            num: this.state.num + 1
        });
    }

    render() {
        return (
            <div>
                <Number num={this.state.num} onChangeNumber={()=>this._plusOne()} />
            </div>
        );
    }
}


class Number extends Component {
    render(){
        return <span>
            <button onClick={() => this.props.onChangeNumber()}>+1</button>
            {this.props.num}
        </span>
    }
}