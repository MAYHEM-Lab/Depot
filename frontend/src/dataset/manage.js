import React, {useState} from 'react';
import EntitySet from "../common/acl";
import API from "../api";
import {useOutletContext} from "react-router";
import {Button, Container, Form, Header, Segment} from "semantic-ui-react";
import util from "../util";
import {DurationInput} from "./creator";

export default function DatasetManage() {
    const {entity, dataset, owner, invalidate} = useOutletContext();

    const originalRetention = dataset.retention ? util.parseDuration(dataset.retention).asMinutes() : null
    const originalSchedule = dataset.schedule ? util.parseDuration(dataset.schedule).asMinutes() : null

    const [retention, setRetention] = useState(originalRetention)
    const [retentionEnabled, setRetentionEnabled] = useState(!!dataset.retention)

    const [schedule, setSchedule] = useState(originalSchedule)
    const [scheduleEnabled, setScheduleEnabled] = useState(!!dataset.schedule)

    const [visibility, setVisibility] = useState(dataset.visibility)
    const [description, setDescription] = useState(dataset.description)

    const changed = originalSchedule !== schedule || originalRetention !== retention || dataset.description !== description || dataset.visibility !== visibility

    const submittable = changed && (!retentionEnabled || retention) && (!scheduleEnabled || schedule)

    const submit = async () => {
        await API.updateDataset(
            entity.name,
            dataset.tag,
            description,
            visibility,
            schedule ? `${schedule}.minutes` : null,
            retention ? `${retention}.minutes` : null
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
                        <Header as='h5' content='Segment TTL'/>
                        <Form.Checkbox checked={retentionEnabled} label='Enable automatic segment expiration' onChange={(e, d) => {
                            if (d.checked) {
                                setRetentionEnabled(true)
                                setRetention(originalRetention)
                            } else {
                                setRetentionEnabled(false)
                                setRetention(null)
                            }
                        }}/>

                        <DurationInput placeholder='Duration' disabled={!retentionEnabled} onSelect={setRetention} minutes={retention}/>

                        <Header as='h5' content='Materialization Schedule'/>
                        <Form.Checkbox checked={scheduleEnabled} label='Enable automatic segment materialization' onChange={(e, d) => {
                            if (d.checked) {
                                setScheduleEnabled(true)
                                setSchedule(originalSchedule)
                            } else {
                                setScheduleEnabled(false)
                                setSchedule(null)
                            }
                        }}/>
                        <DurationInput placeholder='Schedule' disabled={!scheduleEnabled} onSelect={setSchedule} minutes={schedule}/>
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
