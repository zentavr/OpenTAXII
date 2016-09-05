import React, { Component } from 'react';

var resourceToComponent = {
    'collections': Collection,
    'services': Service
};

export default class ResourceList extends Component {
    // initial state
    state = {
        resources: [],
    };

    componentWillMount(){
        fetch(`http://localhost:9000/api/${this.props.resource_type}/`)
            .then(response => response.json())
            .then(json => this.setState({
                resources: json['data']
            }));
    }

    render() {
        return (
            <div>
                <span className="resource-list-title">{this.props.resource_type}</span>
                <ul>
                    {this.state.resources.map((item) => {
                        return (
                            (this.props.resource_type == 'collections' && <Collection key={item.id} resource={item} />)
                            ||
                            (this.props.resource_type == 'services' && <Service key={item.id} resource={item} />)
                            ||
                            (this.props.resource_type == 'content-blocks' && <ContentBlock key={item.id} resource={item} />))
                    })}
                </ul>
            </div>
        );
    }
}

class Resource extends Component {
    render(){
        return (
            <li> Resource {this.props.resource.id} </li>
        );
    }
}


class Service extends Component {
    render(){
        return (
            <li>
                <code>{this.props.resource.id}, {this.props.resource.service_type}</code>
                <pre>{JSON.stringify(this.props.resource.properties, null, 2) }</pre>
            </li>
        );
    }
}

class Collection extends Component {
    render(){
        return (
            <li>
                <ul>
                    <li>{this.props.resource.name}</li>
                    {this.props.resource.description && 
                        <li>{this.props.resource.description}</li>}
                    {this.props.resource.supported_content_bindings.length > 0 && 
                        <li><code>
                            {this.props.resource.supported_content_bindings.map((item) => {
                                return (`${item.binding}` + item.subtypes.join(','))
                            }).join('; ')}
                        </code></li>}
                </ul>
            </li>
        );
    }
}


class ContentBlock extends Component {
    render(){
        return (
            <li>
                <code>{item.content_bindings}</code>
                <pre>{item.content}</pre>
            </li>
        );
    }
}
