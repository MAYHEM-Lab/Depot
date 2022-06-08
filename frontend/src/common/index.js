import React, {Component} from "react";
import {Form, Icon, Input, Label} from "semantic-ui-react";

export class ValidatingInput extends Component {
    VALIDATE_PERIOD = 2000
    DELAY_PERIOD = 500

    // Dispatch async validations no more than a certain rate
    validateTask = null
    lastValidate = new Date(0)

    // Don't dispatch async validations while user is still typing
    delayTask = null
    lastDelay = new Date(0)

    state = {
        data: '',
        validating: false,
        error: false
    }

    handleInput = async (data) => {
        const {sync, async, onValidate, onInput} = this.props

        this.setState({data: data})
        onInput(data)

        if (data === '') {
            this.setState({error: null, validating: false})
            onValidate(true)
            if (this.validateTask) clearTimeout(this.validateTask)
            if (this.delayTask) clearTimeout(this.delayTask)
        } else {

            const err = sync(data)
            if (err) {
                this.setState({error: err, validating: false})
                onValidate(true)
            } else {
                this.setState({error: null, validating: true})
                onValidate(false)
                if (this.delayTask) clearTimeout(this.delayTask)
                this.delayTask = setTimeout(async () => {

                    const elapsed = (new Date() - this.lastValidate)
                    const delay = Math.max(0, this.VALIDATE_PERIOD - elapsed)

                    if (this.validateTask) clearTimeout(this.validateTask)
                    this.validateTask = setTimeout(async () => {
                        this.lastValidate = new Date()
                        const asyncErr = await async(data)
                        if (this.state.data === data) {
                            this.setState({error: asyncErr, validating: false})
                            onValidate(!asyncErr)
                        }
                    }, delay)
                }, this.DELAY_PERIOD)
            }
        }
    }

    render() {
        const {data, validating, error} = this.state
        const {required, placeholder, label} = this.props
        let icon = null
        if (!validating) {
            if (error) icon = 'warning sign'
            else if (data.length) icon = 'checkmark'
        }

        return <Form.Field error={!!error} required={required}>
            <label>{label}</label>
            <Input
                autoFocus
                loading={validating}
                placeholder={placeholder}
                icon={<Icon name={icon} color={error ? 'red' : 'green'}/>}
                onChange={(e, d) => this.handleInput(d.value)}
            />
            {error ? <Label pointing prompt>
                {error}
            </Label> : null}
        </Form.Field>
    }
}