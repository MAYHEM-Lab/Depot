import React from 'react';
import {Icon} from "semantic-ui-react";

export default function EntityIcon({type}) {
    if (type === 'User') {
        return <Icon name='user'/>
    } else {
        return <Icon name='building'/>
    }
}
