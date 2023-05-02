import {Button, Form, Header, Icon, List, Loader, Modal, Search, Segment} from "semantic-ui-react";
import React, {useEffect, useState} from "react";
import API from "../api";
import {Link} from "react-router-dom";
import EntityIcon from "../entity/icon";

function UserEntry({entity, existing, onSelect, existsMsg, addMsg}) {
    const text = existing ? existsMsg : addMsg
    const handleClick = (e) => {
        if (!existing) {
            onSelect(entity.name)
        }
    }
    return <div onClick={handleClick} className={'result' + (existing ? ' add-user-disabled' : '')}>
        <EntityIcon type={entity.type}/>
        {entity.name}<span className='add-user-description'> • {text}</span>
    </div>
}

function MemberAdder({existing, open, onClose, onAdd, usersOnly, existsMsg, addMsg, addTitle}) {
    const [selected, setSelected] = useState(null)
    const [value, setValue] = useState('')
    const [loading, setLoading] = useState(false)
    const [results, setResults] = useState([])
    const search = async (e, d) => {
        setValue(d.value)
        if (d.value) {
            setLoading(true)
            try {
                const {entities} = await API.search(usersOnly, d.value)
                setResults(entities.map(entity => {
                    return {
                        title: entity.name,
                        as: UserEntry,
                        existsMsg: existsMsg,
                        addMsg: addMsg,
                        entity: entity,
                        existing: !existing || existing.some(e => e.entity.name === entity.name),
                        onSelect: setSelected
                    }
                }))
            } finally {
                setLoading(false)
            }
        }
    }

    return <Modal
        onMount={() => {
            setSelected(null)
            setValue('')
            setLoading(false)
            setResults([])
        }}
        dimmer='inverted'
        centered={false}
        size='mini'
        open={open}
        onClose={onClose}
    >
        <Modal.Content className='add-user-menu'>
            <Header>{addTitle}</Header>
            <Form onSubmit={async () => {
                setLoading(true)
                try {
                    await onAdd(selected)
                } finally {
                    setLoading(false)
                }
            }}>
                <Form.Field>
                    {selected ?
                        <div className='selected-user'>
                                <span className='selected-user-title'>
                                    <Icon name='user'/>
                                    {selected}
                                </span>
                            <span className='selected-user-button' onClick={() => setSelected(null)}>✖</span>
                        </div>
                        :
                        <Search
                            fluid
                            minCharacters={1}
                            autoFocus={true}
                            loading={loading}
                            placeholder='Search by username'
                            onSearchChange={search}
                            open={!!value && !loading}
                            results={results}
                            value={value}
                        />
                    }
                </Form.Field>
                <Form.Button disabled={!selected} primary loading={loading} type='submit'>{addTitle}</Form.Button>
            </Form>
        </Modal.Content>
    </Modal>
}

function Member({owner, member, disable, role, onRemove}) {
    return <List.Item>
        <List.Content>
            <div className='member-entry'>
                <div className='member-icon'>
                    <EntityIcon type={member.type}/>
                </div>
                <div className='member-name'>
                    <Link to={{pathname: `/${member.name}`}}>
                        <code>{member.name}</code>
                    </Link>
                </div>
                <div className='member-buttons'>
                    <span className='member-role'>{role}</span>
                    {owner ? <Button disabled={disable} negative={!disable} size='mini' onClick={onRemove}>Remove</Button> : null}
                </div>
            </div>
        </List.Content>
    </List.Item>
}

export default function EntitySet({entity, disable, owner, title, getEntities, addEntity, removeEntity, existsMsg, addMsg, addTitle, usersOnly}) {
    const [entities, setEntities] = useState(null)
    const [addingEntity, setAddingEntity] = useState(false)
    const [refreshKey, setRefreshKey] = useState(0)
    const refresh = () => {
        setRefreshKey(refreshKey + 1)
        setEntities(null)
    }
    useEffect(async () => {
        setEntities(await getEntities())
    }, [entity, refreshKey]);
    return <Segment basic>

        <MemberAdder
            existing={entities}
            open={addingEntity}
            onClose={() => setAddingEntity(false)}
            onAdd={async (member) => {
                await addEntity(member)
                setAddingEntity(false)
                refresh()
            }}
            usersOnly={usersOnly}
            existsMsg={existsMsg}
            addMsg={addMsg}
            addTitle={addTitle}
        />
        <Loader active={!entities}/>
        <List divided>
            <List.Header className='members-header'>
                <Header>
                    {title}
                    {owner
                        ? <Button size='small' primary floated='right' onClick={() => setAddingEntity(true)}>{addTitle}</Button>
                        : null
                    }
                </Header>
            </List.Header>
            {entities ? entities.map(({entity, role}) =>
                <Member
                    key={entity.name}
                    owner={owner}
                    disable={disable === entity.name}
                    member={entity}
                    role={role}
                    onRemove={async () => {
                        await removeEntity(entity.name)
                        refresh()
                    }}
                />
            ) : null}
        </List>
    </Segment>
}