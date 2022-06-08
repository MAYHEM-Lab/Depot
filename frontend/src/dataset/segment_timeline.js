import React, {useEffect, useState} from 'react';
import {useOutletContext, useParams} from "react-router-dom";

import util from '../util'
import API from '../api'
import {Feed, Loader} from "semantic-ui-react";
import SegmentState from "./segment_state";

function Trigger({type}) {
    if (type === 'Creation') {
        return 'Triggered by creation of dataset'
    }
    if (type === 'Scheduled') {
        return 'Triggered by schedule'
    }
    if (type === 'Manual') {
        return 'Triggered manually'
    }
    if (type === 'Upstream') {
        return 'Triggered by upstream segment'
    }
    if (type === 'Downstream') {
        return 'Triggered by downstream segment'
    }
    return null;
}

function Announced({payload}) {
    const {trigger} = payload
    return Trigger(trigger)
}

function Awaiting({payload}) {
    const {trigger} = payload
    return Trigger(trigger)
}

function Unknown() {
    return null
}

const transitions = {
    'Announced': Announced,
    'Awaiting': Awaiting
}

export default function SegmentTimeline() {
    const {version} = useParams()
    const {entity, dataset} = useOutletContext();
    const [history, setHistory] = useState(null)
    useEffect(() => {
        API.getHistory(entity.name, dataset.tag, version).then(history => setHistory(history));
    }, [entity, dataset, version])


    return <div>
        {!history ? <Loader active/> :
            <Feed>
                {Object.keys(history).sort().map((time) => {
                    const data = history[time]
                    const Element = transitions[data.to] || Unknown
                    return <Feed.Event key={time + '-label'}>
                        <Feed.Content>
                            <Feed.Date>
                                <div className='segment-history-entry'>
                                    <div className='segment-history-date'>
                                        {util.formatTime(parseInt(time))}
                                    </div>
                                    <div className='segment-history-state'>
                                        <SegmentState segmentState={data.to}/>
                                    </div>
                                </div>
                            </Feed.Date>
                            <div className='segment-history-description'>
                                <Element key={time + '-text'} payload={data}/>
                            </div>
                        </Feed.Content>
                    </Feed.Event>
                })}
            </Feed>
        }
    </div>
}
