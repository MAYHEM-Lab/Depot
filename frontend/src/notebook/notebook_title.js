import React from 'react';

export default function NotebookTitle({tag, local, selected, onSelect, onClose}) {
    return <div
        className={'notebook-tab-item' + (selected ? ' selected' : '')}
        onClick={onSelect}
    >
        <span className={'notebook-tab-title' + (local ? ' anonymous' : '')}>
            {local ? 'Anonymous Notebook' : tag}
        </span>
        <span className='notebook-tab-button' onClick={onClose}>✖</span>
    </div>
}