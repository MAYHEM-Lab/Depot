import React, {Component} from "react";
import {Container} from "semantic-ui-react";

export default class NotebookStatus extends Component {
    constructor(props) {
        super(props)
        const {widget} = this.props

        this.subscribe(widget.context.sessionContext, widget.context.model)
        this.state = {
            status: widget.context.sessionContext.kernelDisplayStatus,
            dirty: widget.context.model.dirty
        }
    }

    componentDidUpdate(prevProps, prevState, snapshot) {
        const {widget} = this.props
        if (widget !== prevProps.widget) {
            this.unsubscribe(prevProps.widget.context.sessionContext, prevProps.widget.context.model)
            this.subscribe(widget.context.sessionContext, widget.context.model)
            this.setState({
                status: widget.context.sessionContext.kernelDisplayStatus,
                dirty: widget.context.model.dirty
            })
        }
    }

    componentWillUnmount() {
        const {widget} = this.props
        this.unsubscribe(widget.context.sessionContext, widget.context.model)
    }

    subscribe = (sessionContext, model) => {
        sessionContext.connectionStatusChanged.connect(this.sessionUpdated)
        sessionContext.statusChanged.connect(this.sessionUpdated)
        model.stateChanged.connect(this.documentUpdated)
    }

    unsubscribe = (sessionContext, model) => {
        sessionContext.connectionStatusChanged.disconnect(this.sessionUpdated)
        sessionContext.statusChanged.disconnect(this.sessionUpdated)
        model.stateChanged.disconnect(this.documentUpdated)
    }

    sessionUpdated = (sender) => {
        this.setState({status: sender.kernelDisplayStatus})
    }

    documentUpdated = (sender) => {
        this.setState({dirty: sender.dirty})
    }


    render() {
        const {status} = this.state;
        return (
            <Container>
                <div>{status}</div>
            </Container>
        )
    }
}
