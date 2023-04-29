import React, {Component} from 'react';
import {Form, Header, Modal} from "semantic-ui-react";

import API from '../api'
import {ValidatingInput} from "../common";
import validateTag from "../common/validate";

export default class StreamNotebookCreator extends Component {
    state = {
        tag: '',
        topic: '',
        tagValid: false,
        loading: false
    }

    render() {
        const {notebookId, open, onClose, onCreate} = this.props
        const {tag, loading, tagValid} = this.state

        return <Modal
            onMount={() => this.setState({tag: '', loading: false, tagValid: false})}
            dimmer='inverted'
            centered={false}
            size='mini'
            open={open}
            onClose={onClose}
        >
            <Modal.Content className='notebook-creator-menu'>
                <Header>Save Notebook</Header>
                <Form onSubmit={async () => {
                    this.setState({loading: true})
                    try {
                        await onCreate(notebookId, tag)
                    } finally {
                        this.setState({loading: false})
                    }
                }}>
                    <ValidatingInput
                        placeholder='ID'
                        onValidate={(valid) => this.setState({tagValid: valid})}
                        onInput={(input) => this.setState({tag: input})}
                        sync={validateTag}
                        async={async (tag) => {
                            const validate = await API.validateNotebookTag(tag)
                            if (!validate.valid) return 'A notebook with this ID already exists'
                            return null
                        }}
                    />
                    <ValidatingInput
                        placeholder='Topic to subscribe notebook to'
                        onValidate={(valid) => this.setState({tagValid: valid})}
                        onInput={(input) => this.setState({topic: input})}
                        sync={validateTag}
                        async={async (tag) => {
                            if (!true) return 'A notebook with this ID already exists'
                            return null
                        }}
                    />
                    <Form.Button disabled={!tag.length || !tagValid} primary loading={loading} type='submit'>Create</Form.Button>
                </Form>
            </Modal.Content>
        </Modal>
    }
}