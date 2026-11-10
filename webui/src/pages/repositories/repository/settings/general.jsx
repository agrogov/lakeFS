import React, { useEffect, useRef, useState } from 'react';
import { useOutletContext } from 'react-router-dom';
import { useRefs } from '../../../../lib/hooks/repo';
import Container from 'react-bootstrap/Container';
import Row from 'react-bootstrap/Row';
import Button from 'react-bootstrap/Button';
import Form from 'react-bootstrap/Form';
import Alert from 'react-bootstrap/Alert';
import { TrashIcon } from '@primer/octicons-react';
import Col from 'react-bootstrap/Col';
import { AlertError, Loading } from '../../../../lib/components/controls';
import Modal from 'react-bootstrap/Modal';
import { repositories } from '../../../../lib/api';
import { useRouter } from '../../../../lib/hooks/router';
import { useConfigContext } from '../../../../lib/hooks/configProvider';
import { ReadOnlyBadge } from '../../../../lib/components/badges';
import { getRepoStorageConfig } from '../utils';

const BACKGROUND_DELETION_NOTICE = 'Deleting objects in the background';
const BACKGROUND_DELETION_REDIRECT_DELAY_MS = 1500;

const DeleteRepositoryModal = ({ repo, show, onSubmit, onCancel, canDeleteStorage, notice }) => {
    const [isDisabled, setIsDisabled] = useState(true);
    const [deleteStorage, setDeleteStorage] = useState(false);
    const repoNameField = useRef(null);

    const compareRepoName = () => {
        setIsDisabled(repoNameField.current.value !== repo.id);
    };

    const onHide = () => {
        setIsDisabled(true);
        setDeleteStorage(false);
        onCancel();
    };

    return (
        <Modal show={show} onHide={onHide} size="lg">
            <Modal.Header closeButton>
                <Modal.Title>Delete Repository</Modal.Title>
            </Modal.Header>
            <Modal.Body>
                {notice ? (
                    <Alert variant="success" className="mb-0">
                        {notice}
                    </Alert>
                ) : (
                    <>
                        Are you sure you wish to delete repository <strong>{repo.id}</strong>? <br />
                        This action cannot be undone. This will delete the following: <br /> <br />
                        <ul>
                            <li>All commits</li>
                            <li>All branches</li>
                            <li>All tags</li>
                            <li>All repository configuration</li>
                        </ul>
                        {canDeleteStorage && (
                            <Form.Check
                                id="delete-storage"
                                type="checkbox"
                                className="mb-3"
                                checked={deleteStorage}
                                onChange={(event) => setDeleteStorage(event.target.checked)}
                                label={
                                    <>
                                        Also delete all objects in <strong>{repo.storage_namespace}</strong>
                                    </>
                                }
                            />
                        )}
                        {deleteStorage ? (
                            <>
                                All objects under <strong>{repo.storage_namespace}</strong> will be deleted in the
                                background, after the repository metadata is removed. Objects referenced from other
                                repositories will break, and the deletion cannot be cancelled. <br /> <br />
                            </>
                        ) : (
                            <>
                                Data in the underlying object store will not be deleted by this action. <br /> <br />
                            </>
                        )}
                        Please type <strong>{repo.id}</strong> to confirm: <br />
                        <Form.Control
                            className="mt-2"
                            placeholder="Enter repository name to confirm"
                            type="text"
                            autoFocus
                            ref={repoNameField}
                            onChange={compareRepoName}
                        />
                    </>
                )}
            </Modal.Body>
            <Modal.Footer>
                {!notice && (
                    <Button disabled={isDisabled} variant="danger" onClick={() => onSubmit({ deleteStorage })}>
                        I understand the consequences, delete this repository
                    </Button>
                )}
            </Modal.Footer>
        </Modal>
    );
};

const SettingsContainer = () => {
    const router = useRouter();
    const { repo, loading, error } = useRefs();
    const { config } = useConfigContext();
    const [showingDeleteModal, setShowDeleteModal] = useState(false);
    const [deletionError, setDeletionError] = useState(null);
    const [backgroundDeletion, setBackgroundDeletion] = useState(false);

    useEffect(() => {
        if (!backgroundDeletion) {
            return undefined;
        }
        const timer = setTimeout(() => router.push('/repositories'), BACKGROUND_DELETION_REDIRECT_DELAY_MS);
        return () => clearTimeout(timer);
    }, [backgroundDeletion, router]);

    if (loading) return <Loading />;
    if (error) return <AlertError error={error} />;
    if (deletionError) return <AlertError error={deletionError} />;

    const { storageConfig } = getRepoStorageConfig(config?.storages, repo);
    const canDeleteStorage = !!storageConfig?.delete_namespace_support;

    return (
        <div className="mt-3 mb-5">
            <div className="section-title">
                <h4>General</h4>
            </div>

            <Container>
                <Row>
                    <Form.Label column md={{ span: 3 }} className="mb-3">
                        &nbsp;
                    </Form.Label>
                    <Col md={{ span: 4 }}>
                        <ReadOnlyBadge readOnly={repo?.read_only} style={{ marginTop: 7 }} />
                    </Col>
                </Row>
                <Row>
                    <Form.Label column md={{ span: 3 }} className="mb-3">
                        Repository name
                    </Form.Label>
                    <Col md={{ span: 4 }}>
                        <Form.Control readOnly value={repo.id} type="text" />
                    </Col>
                </Row>
                {repo.storage_id && repo.storage_id.length && (
                    <Row>
                        <Form.Label column md={{ span: 3 }} className="mb-3">
                            Storage
                        </Form.Label>
                        <Col md={{ span: 4 }}>
                            <Form.Control readOnly value={repo.storage_id} type="text" />
                        </Col>
                    </Row>
                )}
                <Row>
                    <Form.Label column md={{ span: 3 }} className="mb-3">
                        Storage namespace
                    </Form.Label>
                    <Col md={{ span: 4 }}>
                        <Form.Control readOnly value={repo.storage_namespace} type="text" />
                    </Col>
                </Row>
                <Row>
                    <Form.Label column md={{ span: 3 }} className="mb-3">
                        Default branch
                    </Form.Label>
                    <Col md={{ span: 4 }}>
                        <Form.Control readOnly value={repo.default_branch} type="text" />
                    </Col>
                </Row>
            </Container>

            <Button
                variant="danger"
                className="mt-3"
                disabled={repo?.read_only}
                onClick={() => setShowDeleteModal(!showingDeleteModal)}
            >
                <TrashIcon /> Delete Repository
            </Button>

            <DeleteRepositoryModal
                repo={repo}
                canDeleteStorage={canDeleteStorage}
                notice={backgroundDeletion ? BACKGROUND_DELETION_NOTICE : null}
                onCancel={() => {
                    setShowDeleteModal(false);
                }}
                onSubmit={(options) => {
                    repositories
                        .delete(repo.id, options)
                        .then((result) => {
                            if (result.inBackground) {
                                setBackgroundDeletion(true);
                                return;
                            }
                            router.push('/repositories');
                        })
                        .catch((err) => {
                            setDeletionError(err);
                            setShowDeleteModal(true);
                        });
                }}
                show={showingDeleteModal}
            />
        </div>
    );
};

const RepositoryGeneralSettingsPage = () => {
    const [setActiveTab] = useOutletContext();
    useEffect(() => setActiveTab('general'), [setActiveTab]);
    return <SettingsContainer />;
};

export default RepositoryGeneralSettingsPage;
