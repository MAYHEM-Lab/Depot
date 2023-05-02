import React, {Component} from "react";

import './auth.css'
import API from '../api'
import {Button, Container, Dropdown, Form, Header, Icon, Loader, Message, Modal} from "semantic-ui-react";
import {ValidatingInput} from "../common";
import {Link} from "react-router-dom";
import validateTag from "../common/validate";

function AuthButton({icon, text}) {
    return <Button icon labelPosition='left' secondary>
        <Icon name={icon} size='big' className='login-icon'/>
        {text} with GitHub
    </Button>
}


class GithubRegister extends Component {
    state = {
        loading: false,
        error: false,
        username: '',
        usernameValid: false
    }

    submit = () => {
        const {username} = this.state
        const {registerToken, onRegister} = this.props
        this.setState({loading: true})
        API.githubRegister(username, registerToken)
            .then((response) => onRegister(response.user))
            .catch(() => this.setState({error: true}))
            .finally(() => this.setState({loading: false}))
    }

    render() {
        const {loading, error, username, usernameValid} = this.state
        const {open, onClose} = this.props

        return <Modal
            dimmer='inverted'
            centered={false}
            size='mini'
            open={open}
            onMount={() => this.setState({loading: false, error: null, username: '', usernameValid: false})}
            onClose={onClose}>
            <Modal.Content>
                <Container textAlign='center'>
                    <Header content='Register'/>
                    <Form loading={loading} onSubmit={this.submit} error={error}>
                        <ValidatingInput
                            placeholder='Username'
                            onValidate={(valid) => this.setState({usernameValid: valid})}
                            onInput={(input) => this.setState({username: input})}
                            sync={validateTag}
                            async={async (username) => {
                                const validate = await API.validateUsername(username)
                                if (!validate.valid) return 'This username has been taken already'
                                return null
                            }}
                        />
                        <Form.Button disabled={!username.length || !usernameValid} primary loading={loading} type='submit'>Register</Form.Button>
                        {error ? <Message attached='bottom' compact error>Registration failed</Message> : null}
                    </Form>
                </Container>
            </Modal.Content>
        </Modal>
    }
}

export const UserContext = React.createContext(null)

export default class Authenticator extends Component {
    state = {
        user: null
    }

    render() {
        const {user} = this.state;
        return <>
            <UserContext.Provider value={user}>
                {this.props.children}
            </UserContext.Provider>
            <AuthManager onUser={(user) => this.setState({user: user})} user={user}/>
        </>
    }
}

class AuthManager extends Component {

    state = {
        user: null,
        registering: false,
        loading: true,
        authWindow: null,
        authUrl: null
    }

    logout = async () => {
        await API.logout()
        this.props.onUser(null)
    }

    authenticate = async () => {
        this.setState({loading: true})
        try {
            const {user} = await API.auth()
            this.props.onUser(user)
        } finally {
            this.setState({loading: false})
        }
    }

    githubMessageHandler = async (event) => {
        if (event.data.auth) {
            const {code} = event.data.auth
            this.setState({loading: true})
            const {user_id, token} = await API.githubAuth(code)
            if (user_id) {
                await this.authenticate()
            } else {
                this.setState({registering: true, registerToken: token, loading: false})
            }
        }
    }

    githubAuth = async () => {
        const {authWindow} = this.state
        window.removeEventListener('message', this.githubMessageHandler)
        const strWindowFeatures = 'toolbar=no, menubar=no, width=600, height=700, top=100, left=100'
        const url = await API.githubUrl()
        const name = 'depot-github-auth'

        if (authWindow === null || authWindow.closed) {
            const newWindow = window.open(url, name, strWindowFeatures)
            this.setState({authWindow: newWindow})
        } else {
            const newWindow = window.open(url, name, strWindowFeatures)
            this.setState({authWindow: newWindow})
            authWindow.focus()
        }
        this.setState({authUrl: url})

        window.addEventListener('message', this.githubMessageHandler, false);
    }

    componentDidMount() {
        this.authenticate()
    }

    render() {
        const {loading, registering, registerToken} = this.state
        const {user} = this.props

        return <span className='account-info'>
            <GithubRegister
                registerToken={registerToken}
                open={registering}
                onClose={() => this.setState({registering: false})}
                onRegister={(user) => {
                    this.props.onUser(user)
                    this.setState({registering: false})
                }}
            />
            {loading ?
                <span className='login-loader'>
                    <Loader className='login-loader' active inline='centered' size='tiny'/>
                </span> :
                user ?
                    <Dropdown direction='left' icon={null} className='account-dropdown' trigger={
                        <span className='user-button' onClick={() => this.setState({userDropdownOpen: true})}>
                            <Icon name='user'/>
                            <span className='sign-in-text'><strong>{user.name}</strong></span>
                            <Icon className='range-dropdown-icon' name='angle down'/>
                        </span>
                    }>
                        <Dropdown.Menu>
                            <Dropdown.Item disabled text={<span>Signed in as <strong>{user.name}</strong></span>}/>
                            <Dropdown.Item icon='user' text='Profile' as={Link} to={`/${user.name}`}/>
                            <Dropdown.Item icon='power' text='Sign out' onClick={this.logout}/>
                        </Dropdown.Menu>
                    </Dropdown>
                    :
                    <Dropdown
                        direction='left'
                        icon={null}
                        className='account-dropdown'
                        trigger={
                            <span className='login-button'>
                            <Icon name='user'/><span className='sign-in-text'>Sign in</span>
                            </span>
                        }
                    >
                        <Dropdown.Menu>
                            <Dropdown.Item onClick={this.githubAuth}>
                                <AuthButton icon='github' text='Continue'/>
                            </Dropdown.Item>
                        </Dropdown.Menu>
                    </Dropdown>
            }
        </span>
    }
}