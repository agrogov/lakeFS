import React, { FC, useState } from 'react';
import Badge from 'react-bootstrap/Badge';
import Button from 'react-bootstrap/Button';
import Col from 'react-bootstrap/Col';
import Form from 'react-bootstrap/Form';
import InputGroup from 'react-bootstrap/InputGroup';
import Row from 'react-bootstrap/Row';
import { PlusIcon, XIcon } from '@primer/octicons-react';

import { MetadataKey } from '../../../lib/api/datasets';
import { MetadataDraft, MetadataProblems } from './editorModel';

/**
 * The metadata form generated from the metadata keys (`GET /datasets/metadata-keys`, §5.5,
 * §11.2): required markers, enum dropdowns, a date picker, a number input, a boolean toggle and
 * multi-value chips for `allow_multiple` keys.
 *
 * The form is controlled: the page owns the draft and the problems, so the same validation runs
 * before publishing and is unit tested without a DOM.
 */

export interface MetadataFormProps {
    keys: MetadataKey[];
    draft: MetadataDraft;
    problems: MetadataProblems;
    onChange: (name: string, value: string | string[]) => void;
    disabled?: boolean;
}

const typeHint = (key: MetadataKey): string => {
    const type =
        key.type === 'date' ? 'date (YYYY-MM-DD)' : key.type === 'enum' ? 'one of the allowed values' : key.type;
    return key.allow_multiple ? `multiple ${type} values` : type;
};

/** Removable chips of an allow_multiple value, plus the control that adds a new chip. */
const ChipsField: FC<{
    metadataKey: MetadataKey;
    values: string[];
    onChange: (values: string[]) => void;
    disabled?: boolean;
}> = ({ metadataKey, values, onChange, disabled }) => {
    const [pending, setPending] = useState('');

    const add = () => {
        const value = pending.trim();
        if (value === '') return;
        onChange([...values, value]);
        setPending('');
    };
    const remove = (index: number) => onChange(values.filter((_, i) => i !== index));

    const addLabel = `Add ${metadataKey.name} value`;
    const input =
        metadataKey.type === 'enum' ? (
            <Form.Select
                aria-label={addLabel}
                value={pending}
                disabled={disabled}
                onChange={(e) => setPending(e.target.value)}
            >
                <option value="">Select a value…</option>
                {(metadataKey.allowed_values ?? []).map((allowed) => (
                    <option key={allowed} value={allowed}>
                        {allowed}
                    </option>
                ))}
            </Form.Select>
        ) : (
            <Form.Control
                aria-label={addLabel}
                type={metadataKey.type === 'number' ? 'number' : metadataKey.type === 'date' ? 'date' : 'text'}
                step={metadataKey.type === 'number' ? 'any' : undefined}
                value={pending}
                disabled={disabled}
                onChange={(e) => setPending(e.target.value)}
                onKeyDown={(e) => {
                    if (e.key === 'Enter') {
                        e.preventDefault();
                        add();
                    }
                }}
            />
        );

    return (
        <div className="dataset-metadata-chips">
            {values.length > 0 && (
                <div className="d-flex flex-wrap gap-1 mb-2">
                    {values.map((value, index) => (
                        <Badge
                            key={`${value}-${index}`}
                            bg="light"
                            text="dark"
                            className="border d-inline-flex align-items-center gap-1"
                        >
                            {value}
                            <Button
                                variant="link"
                                size="sm"
                                className="p-0 text-muted"
                                aria-label={`Remove ${value}`}
                                disabled={disabled}
                                onClick={() => remove(index)}
                            >
                                <XIcon size={12} />
                            </Button>
                        </Badge>
                    ))}
                </div>
            )}
            <InputGroup size="sm" className="dataset-metadata-chip-input">
                {input}
                <Button variant="outline-secondary" disabled={disabled || pending.trim() === ''} onClick={add}>
                    <PlusIcon size={12} className="me-1" />
                    Add
                </Button>
            </InputGroup>
        </div>
    );
};

/** One scalar control for the key type. */
const ScalarField: FC<{
    metadataKey: MetadataKey;
    value: string;
    onChange: (value: string) => void;
    disabled?: boolean;
}> = ({ metadataKey, value, onChange, disabled }) => {
    const id = `dataset-metadata-${metadataKey.name}`;

    if (metadataKey.type === 'boolean') {
        return (
            <Form.Check
                id={id}
                type="switch"
                label={value === 'true' ? 'true' : 'false'}
                checked={value === 'true'}
                disabled={disabled}
                onChange={(e) => onChange(e.target.checked ? 'true' : 'false')}
            />
        );
    }
    if (metadataKey.type === 'enum') {
        return (
            <Form.Select id={id} value={value} disabled={disabled} onChange={(e) => onChange(e.target.value)}>
                <option value="">Select a value…</option>
                {(metadataKey.allowed_values ?? []).map((allowed) => (
                    <option key={allowed} value={allowed}>
                        {allowed}
                    </option>
                ))}
            </Form.Select>
        );
    }
    return (
        <Form.Control
            id={id}
            type={metadataKey.type === 'number' ? 'number' : metadataKey.type === 'date' ? 'date' : 'text'}
            step={metadataKey.type === 'number' ? 'any' : undefined}
            value={value}
            disabled={disabled}
            onChange={(e) => onChange(e.target.value)}
        />
    );
};

export const MetadataForm: FC<MetadataFormProps> = ({ keys, draft, problems, onChange, disabled }) => {
    if (keys.length === 0) {
        return <p className="text-muted">No metadata keys are defined on this server yet.</p>;
    }

    return (
        <div className="dataset-metadata-form">
            {keys.map((key) => {
                const value = draft[key.name];
                const messages = problems[key.name] ?? [];
                const multi = key.allow_multiple;
                return (
                    <Row key={key.name} className="dataset-metadata-field mb-3">
                        <Col md={4}>
                            <Form.Label htmlFor={`dataset-metadata-${key.name}`} className="mb-0">
                                {key.name}
                                {key.required && (
                                    <span className="text-danger ms-1" aria-label="required">
                                        *
                                    </span>
                                )}
                            </Form.Label>
                            <div className="text-muted small">
                                {typeHint(key)}
                                {key.description ? ` — ${key.description}` : ''}
                            </div>
                        </Col>
                        <Col md={8}>
                            {multi ? (
                                <ChipsField
                                    metadataKey={key}
                                    values={Array.isArray(value) ? value : []}
                                    disabled={disabled}
                                    onChange={(next) => onChange(key.name, next)}
                                />
                            ) : (
                                <ScalarField
                                    metadataKey={key}
                                    value={typeof value === 'string' ? value : ''}
                                    disabled={disabled}
                                    onChange={(next) => onChange(key.name, next)}
                                />
                            )}
                            {messages.map((message) => (
                                <div key={message} className="invalid-feedback d-block" role="alert">
                                    {message}
                                </div>
                            ))}
                        </Col>
                    </Row>
                );
            })}
        </div>
    );
};
