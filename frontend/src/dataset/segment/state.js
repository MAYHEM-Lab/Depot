import React from 'react';
import {Label} from "semantic-ui-react";

export default function SegmentState({segmentState, className}) {
    const color = (stateType) => {
        switch (stateType) {
            case 'Initializing': return 'grey';
            case 'Announced':    return 'yellow';
            case 'Awaiting':     return 'orange';
            case 'Queued':       return 'teal';
            case 'Transforming': return 'blue';
            case 'Materialized': return 'green';
            case 'Failed':       return 'red';
            default:             return 'black';
        }
    }
    return <Label className={className} color={color(segmentState)}>
        {segmentState}
    </Label>
}
