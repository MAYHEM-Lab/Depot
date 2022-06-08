import React, {Component} from 'react';
import {Form, Header, Modal} from "semantic-ui-react";

import API from '../api'
import {ValidatingInput} from "../common";
import validateTag from "../common/validate";

export default class OrganizationCreator extends Component {
    state = {
        tag: '',
        tagValid: false,
        loading: false
    }

    render() {
        const {open, onClose, onCreate} = this.props
        const {tag, loading, tagValid} = this.state

        return <Modal
            onMount={() => this.setState({tag: '', loading: false, tagValid: false})}
            dimmer='inverted'
            centered={false}
            size='mini'
            open={open}
            onClose={onClose}
        >
            <Modal.Content className='org-creator-menu'>
                <Header>Create Organization</Header>
                <Form onSubmit={async () => {
                    this.setState({loading: true})
                    try {
                        await onCreate(tag)
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
                            const validate = await API.validateUsername(tag)
                            if (!validate.valid) return 'This name is not available'
                            return null
                        }}
                    />
                    <Form.Button disabled={!tag.length || !tagValid} primary loading={loading} type='submit'>Create</Form.Button>
                </Form>
            </Modal.Content>
        </Modal>
    }
}