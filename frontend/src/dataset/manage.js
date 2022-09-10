import React, {useRef, useState} from 'react';
import EntitySet from "../common/acl";
import API from "../api";
import {useOutletContext} from "react-router";
import {Button, Container, Form, Header, Input, Segment} from "semantic-ui-react";
import util from "../util";

export default function DatasetManage() {
    const {entity, dataset, owner, invalidate} = useOutletContext();

    const originalRetention = dataset.retention ? util.parseDuration(dataset.retention).minutes().toString() : ''
    const originalSchedule = dataset.schedule ? util.parseDuration(dataset.schedule).minutes().toString() : ''

    const retentionInput = useRef(null)
    const [retention, setRetention] = useState(originalRetention)
    const [retentionEnabled, setRetentionEnabled] = useState(!!dataset.retention)

    const scheduleInput = useRef(null)
    const [schedule, setSchedule] = useState(originalSchedule)
    const [scheduleEnabled, setScheduleEnabled] = useState(!!dataset.schedule)

    const [visibility, setVisibility] = useState(dataset.visibility)
    const [description, setDescription] = useState(dataset.description)

    const hasChanged = (before, after) => {
        const a = parseInt(before)
        const b = parseInt(after)
        if (isNaN(a)) return !isNaN(b)
        if (isNaN(b)) return true
        return a !== b
    }

    const changed = hasChanged(originalSchedule, schedule) || hasChanged(originalRetention, retention) || dataset.description !== description || dataset.visibility !== visibility

    const submittable = changed && (!retentionEnabled || util.isNumeric(retention)) && (!scheduleEnabled || util.isNumeric(schedule))

    const submit = async () => {
        await API.updateDataset(
            entity.name,
            dataset.tag,
            description,
            visibility,
            schedule ? `${parseInt(schedule)}.minutes` : null,
            retention ? `${parseInt(retention)}.minutes` : null
        )
        invalidate()
    }

    return <>
        <Segment basic>
            <Header>Settings</Header>
            <Form onSubmit={submit} loading={false}>
                <Header as='h5' content='Visibility'/>
                <Button
                    primary={visibility === 'Public'}
                    attached='left'
                    onClick={() => setVisibility('Public')}
                >
                    Public
                </Button>
                <Button
                    primary={visibility === 'Private'}
                    attached='right'
                    onClick={() => setVisibility('Private')}
                >
                    Private
                </Button>


                <Header as='h5' content='Description'/>
                <Form.TextArea placeholder='Description' value={description} onChange={(e, d) => {
                    setDescription(e.target.value)
                }}/>

                {dataset.origin === 'Managed' ?
                    <>
                        <Header as='h5' content='Retention Policy'/>
                        <Form.Checkbox checked={retentionEnabled} label='Enable automatic data pruning' onChange={(e, d) => {
                            if (d.checked) {
                                setRetentionEnabled(true)
                                setRetention(originalRetention)
                                setTimeout(() => retentionInput.current.focus(), 1);
                            } else {
                                setRetentionEnabled(false)
                                setRetention('')
                            }
                        }
                        }/>
                        <Form.Field inline error={!!retention && !util.isNumeric(retention)}>
                            <Input
                                ref={retentionInput}
                                placeholder='Retention'
                                disabled={!retentionEnabled}
                                value={retention}
                                label={{basic: true, content: 'weeks'}}
                                labelPosition='right'
                                onChange={(e, d) => {
                                    setRetention(e.target.value)
                                }}
                            />
                        </Form.Field>

                        <Header as='h5' content='Materialization Policy'/>
                        <Form.Checkbox checked={scheduleEnabled} label='Enable automatic segment materialization' onChange={(e, d) => {
                            if (d.checked) {
                                setScheduleEnabled(true)
                                setSchedule(originalSchedule)
                                setTimeout(() => scheduleInput.current.focus(), 1);
                            } else {
                                setScheduleEnabled(false)
                                setSchedule('')
                            }
                        }
                        }/>
                        <Form.Field inline error={!!schedule && !util.isNumeric(schedule)}>
                            <Input
                                ref={scheduleInput}
                                placeholder='Frequency'
                                disabled={!scheduleEnabled}
                                value={schedule}
                                label={{basic: true, content: 'minutes'}}
                                labelPosition='right'
                                onChange={(e, d) => {
                                    setSchedule(e.target.value)
                                }}
                            />
                        </Form.Field>
                    </> :
                    null
                }
                <Container className='settings-buttons'>
                    <Button type='submit' disabled={!submittable} positive content='Save changes'/>
                </Container>
            </Form>
        </Segment>

        <EntitySet
            owner={owner}
            entity={entity}
            disable={entity.name}
            getEntities={() => API.getDatasetCollaborators(entity.name, dataset.tag)}
            addEntity={member => API.addDatasetCollaborator(entity.name, dataset.tag, member)}
            removeEntity={member => API.delDatasetCollaborator(entity.name, dataset.tag, member)}
            existsMsg={'Already has access'}
            addMsg={'Add collaborator'}
            addTitle={'Add collaborator'}
            title={'Collaborators'}
            usersOnly={false}
        />
    </>
}
