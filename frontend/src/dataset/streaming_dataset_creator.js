import React, {Component, useEffect, useState} from "react";
import API from "../api";
import {Button, Container, Dropdown, Form, Header, Icon, Input, List, Segment, Select} from "semantic-ui-react";
import {Link} from "react-router-dom";
import {ValidatingInput} from "../common";
import validateTag from "../common/validate";
import util from "../util";

export function VisibilityInput({onSelect}) {
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

export function OwnerInput({onSelect}) {
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

export function DurationInput({onSelect, minutes, disabled, placeholder}) {
    const durations = [
        {key: 'days', text: 'days', value: 60 * 24},
        {key: 'hours', text: 'hours', value: 60},
        {key: 'minutes', text: 'minutes', value: 1},
    ]

    let defaultUnit = 60
    let defaultValue = ''
    if (minutes) {
        for (let i = 0; i < durations.length; i++) {
            if (minutes >= durations[i].value && minutes % durations[i].value === 0) {
                const d = durations[i]
                defaultUnit = d.value
                defaultValue = (Math.floor(minutes / d.value)).toString()
                break;
            }
        }
    }

    const [unit, setUnit] = useState(defaultUnit)
    const [input, setInput] = useState(defaultValue)

    const update = (u, i) => {
        setUnit(u)
        setInput(i)
        if (!!i && util.isNumeric(i)) {
            onSelect(parseInt(i) * u)
        } else {
            onSelect(null)
        }
    }
    return <Form.Field inline error={!!input && !util.isNumeric(input)}>
        <Input
            autoFocus
            onChange={(e, d) => {
                update(unit, d.value)
            }}
            defaultValue={input}
            placeholder={placeholder}
            disabled={disabled}
            label={{
                basic: true,
                className: 'duration-selector-label',
                content: <Select options={durations} defaultValue={unit} onChange={(e, {value}) => update(value, input)}/>
            }}
            labelPosition='right'
        />
    </Form.Field>

}

export function SegmentTTL({onSelect}) {
    const [unbounded, setUnbounded] = useState(false)
    let message
    if (unbounded) {
        message = <Container text className='strategy-container'>
            Segments of this dataset will not be automatically deleted.
        </Container>
    } else {
        message = <div className='strategy-input'>
            <div className='strategy-text'>
                <DurationInput placeholder='Duration' onSelect={r => onSelect({unbounded: unbounded, retention: r})}/>
            </div>
        </div>
    }
    return <>
        <Header as='h3'>Segment TTL</Header>
        <Button
            primary={!unbounded}
            className='strategy-button'
            attached='left'
            onClick={() => {
                setUnbounded(false)
                onSelect({unbounded: false, retention: null})
            }}
        >
            Pruned
        </Button>
        <Button
            primary={unbounded}
            className='strategy-button'
            attached='right'
            onClick={() => {
                setUnbounded(true)
                onSelect({unbounded: true, retention: null})
            }}
        >
            Unbounded
        </Button>
        <div className='dataset-create-step-meta'>
            {message}
        </div>
    </>
}

export function AvailabilityClass({touchedDatasets, onSelect}) {
    const [strong, setStrong] = useState(false)
    let message
    if (!touchedDatasets.length) {
        message = <Container text className='strategy-container'/>
    } else {
        if (strong) {
            message = <Container text className='strategy-container'>
                A segment will force retention of its parent segments as soon as it is announced.
            </Container>
        } else {
            message = <Container text className='strategy-container'>
                A segment will force retention of its parent segments immediately before it is materialized.
            </Container>
        }
    }
    return <>
        <Header as='h3'>Availability Class</Header>
        <Button
            primary={strong && !!touchedDatasets.length}
            disabled={!touchedDatasets.length}
            className='strategy-button'
            attached='left'
            onClick={() => {
                setStrong(true)
                onSelect({storageClass: 'Strong'})
            }}
        >
            Strong
        </Button>
        <Button
            primary={!strong && !!touchedDatasets.length}
            disabled={!touchedDatasets.length}
            className='strategy-button'
            attached='right'
            onClick={() => {
                setStrong(false)
                onSelect({storageClass: 'Weak'})
            }}
        >
            Weak
        </Button>
        <div className='dataset-create-step-meta'>
            {message}
        </div>
    </>
}

export function MaterializationStrategy({onSelect}) {
    const [manual, setManual] = useState(false)
    let message
    if (manual) {
        message = <Container text className='strategy-container'>
            New versions of this dataset will not be automatically materialized.
        </Container>
    } else {
        message = <div className='strategy-input'>
            <div className='strategy-text'>
                <DurationInput placeholder='Schedule' onSelect={f => onSelect({unbounded: manual, frequency: f})}/>
            </div>
        </div>
    }
    return <>
        <Header as='h3'>Materialization Strategy</Header>
        <Button
            primary={manual}
            className='strategy-button'
            attached='left'
            onClick={() => {
                setManual(true)
                onSelect({manual: true, frequency: null})
            }}
        >
            Manual
        </Button>
        <Button
            primary={!manual}
            className='strategy-button'
            attached='right'
            onClick={() => {
                setManual(false)
                onSelect({manual: false, frequency: null})
            }}
        >
            Scheduled
        </Button>
        <div className='dataset-create-step-meta'>
            {message}
        </div>
    </>
}

export function EvaluationStrategy({touchedDatasets, onSelect}) {
    const [triggered, setTriggered] = useState(!!touchedDatasets.length)

    let message
    if (triggered) {
        message = <Container className='strategy-triggers-list'>
            <List bulleted>
                {touchedDatasets.map(({entity, tag}) =>
                    <List.Item key={`${entity}/${tag}`}>
                        <Link to={`/${entity}/datasets/${tag}`}>
                            <code>{`${entity}/${tag}`}</code>
                        </Link>
                    </List.Item>
                )}
            </List>
        </Container>
    } else {
        message = <Container text className='strategy-container'>
            New versions of this dataset will be announced subject to the materialization policy.
        </Container>
    }
    return <>
        <Header as='h3'>Evaluation Strategy</Header>
        <Button
            primary={triggered}
            disabled={!touchedDatasets.length}
            className='strategy-button'
            attached='left'
            onClick={() => {
                setTriggered(true)
                onSelect({triggered: true})
            }}
        >
            Triggered
        </Button>
        <Button
            primary={!triggered}
            className='strategy-button'
            attached='right'
            onClick={() => {
                setTriggered(false)
                onSelect({triggered: false})
            }}
        >
            Isolated
        </Button>
        <div className='dataset-create-step-meta'>
            {message}
        </div>
    </>
}

export default class StreamingDatasetCreator extends Component {
    state = {
        loading: false,
        createdTag: null,
        tag: '',
        unbounded: false,
        retention: '',
        description: '',
        tagValid: false,
        triggered: !!this.props.payload.touchedDatasets.length,
        manual: false,
        visibility: 'Public',
        storageClass: 'Strong',
        clusterAffinity: null,
        frequency: null,
        topic: '',
        window: 0,
        bootstrapServer: '',
        step: 0
    }

    nextPage = () => {
        const {step} = this.state
        this.setState({step: Math.min(step + 1, 3)})
    }

    prevPage = () => {
        const {step} = this.state
        this.setState({step: Math.max(step - 1, 0)})
    }

    createStreamingDataset = async () => {
        const {tag, triggered, frequency, retention, visibility, owner, description, storageClass, topic, window, bootstrapServer} = this.state
        const {payload: {touchedDatasets, resultType}, notebook} = this.props
        this.setState({loading: true})
        try {
            const content = notebook.widget.content.model.toJSON()
            console.log(content)
            content.cells = content.cells.map(cell => {
                return {...cell, outputs: []}
            })
            console.log(topic)
            console.log(window)
            console.log(bootstrapServer)
            await API.createStreamingTopicDataset(
                owner,
                tag,
                description,
                content,
                resultType,
                visibility,
                topic,
                window,
                bootstrapServer
            )
            this.setState({createdTag: tag})
        } finally {
            this.setState({loading: false})
        }
    }

    render() {
        const {createdTag, tagValid, loading, tag, manual, frequency, unbounded, retention, owner, topic, window, bootstrapServer} = this.state
        const {step} = this.state
        const {payload: {touchedDatasets}} = this.props

        const infoValid = tag.length && tagValid && owner && topic.length && window && bootstrapServer
        const matValid = manual || frequency
        const ttlValid = unbounded || retention

        const stepValid = [infoValid, matValid, ttlValid]

        return <Segment className='streaming_dataset_creator' textAlign='center'>
            {createdTag ?
                <>
                    <Icon size='huge' name='check circle outline' color='green'/>
                    <Header>Defined dataset <Link to={`/${owner}/datasets/${createdTag}`}>{`${owner}/${createdTag}`}</Link></Header>
                </> :
                <Form onSubmit={() => step === 2 ? this.createStreamingDataset() : this.nextPage()}>
                    <div className={'dataset-create-step ' + (step === 0 ? '' : 'hidden')}>
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
                                    if (!validate.valid) return 'A streaming dataset with this ID already exists'
                                    return null
                                }}
                            />
                            <OwnerInput onSelect={(owner) => this.setState({owner: owner})}/>
                            <VisibilityInput onSelect={(visibility) => this.setState({visibility: visibility})}/>
                            <Form.TextArea label='Description' onChange={(e, d) => this.setState({description: d.value})}/>
                            <Form.Input label='Topic' onChange={(e,d) => this.setState({topic: d.value})}/>
                            <Form.Input label='Window' onChange={(e,d) => this.setState({window: d.value})}/>
                            <Form.Input label='Bootstrap Server' onChange={(e,d) => this.setState({bootstrapServer: d.value})}/>
                        </Segment>
                    </div>

                    <Segment basic>
                            <Button type='submit' disabled={!(infoValid)} positive loading={loading}>Create</Button>
                    
                    </Segment>
                </Form>
            }
        </Segment>
    }
}
