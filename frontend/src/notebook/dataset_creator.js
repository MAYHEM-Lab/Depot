import React, {Component, useEffect, useState} from "react";
import API from "../api";
import {Button, Container, Dropdown, Form, Grid, Header, Icon, Input, List, Segment} from "semantic-ui-react";
import {Link} from "react-router-dom";
import {ReactWidget} from "@jupyterlab/ui-components";
import {UNSAFE_LocationContext as LocationContext, UNSAFE_NavigationContext as NavigationContext} from "react-router";
import {ValidatingInput} from "../common";
import validateTag from "../common/validate";

export default class DatasetWidget extends ReactWidget {
    payload = null
    notebook = null

    constructor(notebook, location, navigator) {
        super()
        this.notebook = notebook
        this.location = location
        this.navigator = navigator
    }

    renderModel = (model) => {
        const data = model.data['application/depot-publish']
        const datasets = data.touched_datasets.map((info) => {
            const [entity, tag] = info.split('/')
            return {entity: entity, tag: tag}
        })
        this.payload = {
            touchedDatasets: datasets,
            resultType: data.result_type
        }
        return Promise.resolve()
    }

    render() {
        return <LocationContext.Provider value={this.location}>
            <NavigationContext.Provider value={this.navigator}>
                <DatasetCreator payload={this.payload} notebook={this.notebook}/>
            </NavigationContext.Provider>
        </LocationContext.Provider>
    }
}

function VisibilityInput({onSelect}) {
    const [selected, setSelected] = useState('Public')
    const select = (visibility) => {
        setSelected(visibility)
        onSelect(visibility)
    }

    const renderItem = ({name, icon}) => {
        return <Dropdown.Item key={name} value={name} onClick={() => select(name)}>
            <div>
                <Icon name={icon}/>
                {name}
            </div>
        </Dropdown.Item>
    }

    const visibilities = [
        {name: 'Public', icon: 'globe'},
        {name: 'Private', icon: 'lock'}
    ]

    return <Form.Field required={true}>
        <label>Visibility</label>
        <Dropdown trigger={renderItem(visibilities.find((e) => e.name === selected))} value={selected} className='selection'>
            <Dropdown.Menu>
                {visibilities.map(renderItem)}
            </Dropdown.Menu>
        </Dropdown>
    </Form.Field>
}

function OwnerInput({onSelect}) {
    const [entities, setEntities] = useState(null)
    const [selected, setSelected] = useState(null)
    useEffect(async () => {
            const {entities} = await API.getAuthorizedEntities()
            setEntities(entities)
            setSelected(entities[0].id)
            onSelect(entities[0].name)
        }, []
    )

    const select = (entity) => {
        setSelected(entity.id)
        onSelect(entity.name)
    }

    const renderItem = (entity) => {
        if (!entity) return null
        return <Dropdown.Item key={entity.id} value={entity.id} onClick={() => select(entity)}>
            <div>
                <Icon name={entity.type === 'User' ? 'user' : 'building'}/>
                {entity.name}
            </div>
        </Dropdown.Item>
    }

    return <Form.Field required={true}>
        <label>Owner</label>
        <Dropdown loading={!entities} trigger={entities ? renderItem(entities.find((e) => e.id === selected)) : null} value={selected} className='selection'>
            <Dropdown.Menu>
                {entities ? entities.map((entity) => renderItem(entity)) : null}
            </Dropdown.Menu>
        </Dropdown>
    </Form.Field>
}

class DatasetCreator extends Component {
    state = {
        loading: false,
        createdTag: null,
        tag: '',
        description: '',
        tagValid: false,
        triggered: !!this.props.payload.touchedDatasets.length,
        manual: true,
        visibility: 'Public',
        clusterAffinity: null,
        frequency: null
    }

    createDataset = async () => {
        const {tag, triggered, frequency, visibility, owner, description} = this.state
        const {payload: {touchedDatasets, resultType}, notebook} = this.props
        this.setState({loading: true})
        try {
            const content = notebook.widget.content.model.toJSON()
            content.cells = content.cells.map(cell => {
                return {...cell, outputs: []}
            })
            await API.createDataset(
                owner,
                tag,
                description,
                content,
                resultType,
                visibility,
                touchedDatasets,
                !triggered,
                parseInt(frequency)
            )
            this.setState({createdTag: tag})
        } finally {
            this.setState({loading: false})
        }
    }

    evaluationStrategy = () => {
        const {payload: {touchedDatasets}} = this.props
        const {triggered} = this.state

        if (triggered) {
            return <Container className='strategy-triggers-list'>
                <List bulleted>
                    {touchedDatasets.map(({entity, tag}) =>
                        <List.Item key={`${entity}/${tag}`}>
                            <Link to={`/${entity}/${tag}`}>
                                <code>{`${entity}/${tag}`}</code>
                            </Link>
                        </List.Item>
                    )}
                </List>
            </Container>
        } else {
            return <Container className='strategy-container'>
                New versions of this dataset will be announced subject to the materialization policy.
            </Container>
        }
    }

    materializtionStrategy = () => {
        const {manual} = this.state

        if (manual) {
            return <Container className='strategy-container'>
                New versions of this dataset will not be automatically materialized.
            </Container>
        } else {
            return <div className='strategy-input'>
                <div className='strategy-text'>
                    <Form.Field inline error={false}>
                        <Input
                            autoFocus
                            onChange={(e, d) => this.setState({frequency: d.value})}
                            placeholder='Frequency'
                            label={{content: 'minutes'}}
                            labelPosition='right'/>
                    </Form.Field>
                </div>
            </div>
        }
    }

    render() {
        const {createdTag, tagValid, loading, tag, triggered, manual, frequency, owner} = this.state
        const {payload: {touchedDatasets}} = this.props

        return <Segment className='dataset-creator' textAlign='center'>
            {createdTag ?
                <>
                    <Icon size='huge' name='check circle outline' color='green'/>
                    <Header>Created dataset <Link to={`/${owner}/${createdTag}`}>{`${owner}/${createdTag}`}</Link></Header>
                </> :
                <Form onSubmit={this.createDataset}>
                    <Grid padded columns={2}>
                        <Grid.Column>
                            <Header as='h3'>Information</Header>
                            <Segment basic textAlign='left'>
                                <ValidatingInput
                                    required
                                    label='Name'
                                    placeholder='ID'
                                    onValidate={(valid) => this.setState({tagValid: valid})}
                                    onInput={(input) => this.setState({tag: input})}
                                    sync={validateTag}
                                    async={async (tag) => {
                                        const validate = await API.validateDatasetTag(tag)
                                        if (!validate.valid) return 'A dataset with this ID already exists'
                                        return null
                                    }}
                                />
                                <OwnerInput onSelect={(owner) => this.setState({owner: owner})}/>
                                <VisibilityInput onSelect={(visibility) => this.setState({visibility: visibility})}/>
                                <Form.TextArea label='Description' onChange={(e, d) => this.setState({description: d.value})}/>
                            </Segment>
                        </Grid.Column>
                        <Grid.Column className='strategy-settings'>
                            <Header as='h3'>Evaluation Strategy</Header>
                            <Button
                                primary={triggered}
                                disabled={!touchedDatasets.length}
                                className='strategy-button'
                                attached='left'
                                onClick={() => this.setState({triggered: true})}
                            >
                                Triggered
                            </Button>
                            <Button
                                primary={!triggered}
                                className='strategy-button'
                                attached='right'
                                onClick={() => this.setState({triggered: false})}
                            >
                                Isolated
                            </Button>
                            {this.evaluationStrategy()}

                            <Header as='h3'>Materialization Strategy</Header>
                            <Button
                                primary={manual}
                                className='strategy-button'
                                attached='left'
                                onClick={() => this.setState({manual: true, frequency: null})}
                            >
                                Manual
                            </Button>
                            <Button
                                primary={!manual}
                                className='strategy-button'
                                attached='right'
                                onClick={() => this.setState({manual: false})}
                            >
                                Scheduled
                            </Button>
                            {this.materializtionStrategy()}
                        </Grid.Column>
                    </Grid>
                    <Form.Button disabled={!tag.length || (!manual && !frequency) || !tagValid || !owner} positive loading={loading} type='submit'>Create</Form.Button>
                </Form>
            }
        </Segment>
    }
}
