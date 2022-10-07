import React, {useEffect, useState} from 'react';
import {Link, useOutletContext, useParams} from "react-router-dom";

import util from '../../util'
import API from '../../api'
import {Feed, Loader} from "semantic-ui-react";
import SegmentState from './state';

async function Trigger(trigger, verb) {
    const {type} = trigger
    if (type === 'Creation') {
        return (verb || 'Triggered') + ' by creation of dataset'
    }
    if (type === 'Scheduled') {
        return (verb || 'Triggered') + ' by schedule'
    }
    if (type === 'Manual') {
        const user = await API.getEntityById(trigger.who)
        return <>{(verb || 'Triggered') + ' by '}<Link to={`/${user.name}`}>{user.name}</Link></>
    }
    if (type === 'Upstream') {
        return (verb || 'Triggered') + ' by upstream segment'
    }
    if (type === 'Downstream') {
        return (verb || 'Triggered') + ' by downstream segment'
    }
    return null;
}

async function Announced({trigger}) {
    return Trigger(trigger)
}

async function Awaiting({trigger}) {
    return Trigger(trigger)
}

async function Materialized({trigger, shallow_size, retained_size}) {
    const message = await Trigger(trigger, 'Uploaded')
    return <>
        {message}
        <br/>
        <span>Shallow size {util.formatBytes(shallow_size)} - Retained size {util.formatBytes(retained_size)}</span>
    </>
}

async function Unknown() {
    return null
}

const transitions = {
    'Announced': Announced,
    'Awaiting': Awaiting,
    'Materialized': Materialized
}

export default function SegmentTimeline() {
    const {version} = useParams()
    const {entity, dataset} = useOutletContext();
    const [history, setHistory] = useState(null)
    useEffect(async () => {
        const timeline = await API.getHistory(entity.name, dataset.tag, version)

        const history = Object.keys(timeline).sort().map(async (time) => {
            const transition = timeline[time]
            const message = await (transitions[transition.to] || Unknown)(transition)
            return {time: time, message: message, ...transition}
        })
        setHistory(await Promise.all(history))
    }, [entity, dataset, version])


    return <div>
        {!history ? <Loader active/> :
            <Feed>
                {history.map(({time, to, message}) => {
                    return <Feed.Event key={time + '-label'}>
                        <Feed.Content>
                            <Feed.Summary>
                                <Feed.Date>
                                    <div className='segment-history-entry'>
                                        <div className='segment-history-date'>
                                            {util.formatTime(parseInt(time))}
                                        </div>
                                        <div className='segment-history-state'>
                                            <SegmentState segmentState={to}/>
                                        </div>
                                    </div>
                                </Feed.Date>
                                <span className='segment-history-description'>
                                    {message}
                                </span>
                            </Feed.Summary>
                        </Feed.Content>
                    </Feed.Event>
                })}
            </Feed>
        }
    </div>
}
