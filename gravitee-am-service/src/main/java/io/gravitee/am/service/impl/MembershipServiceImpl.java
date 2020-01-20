/**
 * Copyright (C) 2015 The Gravitee team (http://gravitee.io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.gravitee.am.service.impl;

import io.gravitee.am.common.audit.EventType;
import io.gravitee.am.common.event.Action;
import io.gravitee.am.common.event.Type;
import io.gravitee.am.common.utils.RandomString;
import io.gravitee.am.identityprovider.api.User;
import io.gravitee.am.model.Group;
import io.gravitee.am.model.Membership;
import io.gravitee.am.model.Role;
import io.gravitee.am.model.common.event.Event;
import io.gravitee.am.model.common.event.Payload;
import io.gravitee.am.model.membership.Member;
import io.gravitee.am.model.membership.MemberType;
import io.gravitee.am.model.membership.ReferenceType;
import io.gravitee.am.model.permissions.RoleScope;
import io.gravitee.am.repository.management.api.MembershipRepository;
import io.gravitee.am.service.*;
import io.gravitee.am.service.exception.*;
import io.gravitee.am.service.reporter.builder.AuditBuilder;
import io.gravitee.am.service.reporter.builder.management.DomainAuditBuilder;
import io.gravitee.am.service.reporter.builder.management.MembershipAuditBuilder;
import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.Single;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author Titouan COMPIEGNE (titouan.compiegne at graviteesource.com)
 * @author GraviteeSource Team
 */
@Component
public class MembershipServiceImpl implements MembershipService {

    private static final Logger LOGGER = LoggerFactory.getLogger(MembershipServiceImpl.class);

    @Autowired
    private MembershipRepository membershipRepository;

    @Autowired
    private AuditService auditService;

    @Autowired
    private UserService userService;

    @Autowired
    private GroupService groupService;

    @Autowired
    private RoleService roleService;

    @Autowired
    private DomainService domainService;

    @Autowired
    private EventService eventService;

    @Override
    public Maybe<Membership> findById(String id) {
        LOGGER.debug("Find membership by ID {}", id);
        return membershipRepository.findById(id)
                .onErrorResumeNext(ex -> {
                    LOGGER.error("An error occurs while trying to find membership by ID", id, ex);
                    return Maybe.error(new TechnicalManagementException(String.format("An error occurs while trying to find membership by ID %s", id), ex));
                });
    }

    @Override
    public Flowable<Membership> findAll() {
        LOGGER.debug("Find memberships");
        return membershipRepository.findAll()
                .onErrorResumeNext(ex -> {
                    LOGGER.error("An error occurs while trying to find memberships", ex);
                    return Flowable.error(new TechnicalManagementException("An error occurs while trying to find memberships", ex));
                });
    }

    @Override
    public Single<List<Membership>> findByReference(String referenceId, ReferenceType referenceType) {
        LOGGER.debug("Find memberships by reference id {} and reference type {}", referenceId, referenceType);
        return membershipRepository.findByReference(referenceId, referenceType)
                .onErrorResumeNext(ex -> {
                    LOGGER.error("An error occurs while trying to find memberships by reference id {} and reference type {}", referenceId, referenceType, ex);
                    return Single.error(new TechnicalManagementException(
                            String.format("An error occurs while trying to find memberships by reference id %s and reference type %s", referenceId, referenceType), ex));
                });
    }

    @Override
    public Maybe<Membership> findByReferenceAndMember(String referenceId, String memberId) {
        LOGGER.debug("Find memberships by reference id {} and member id {}", referenceId, memberId);
        return membershipRepository.findByReferenceAndMember(referenceId, memberId)
                .onErrorResumeNext(ex -> {
                    LOGGER.error("An error occurs while trying to find memberships by reference id {} and member id {}", referenceId, memberId, ex);
                    return Maybe.error(new TechnicalManagementException(
                            String.format("An error occurs while trying to find memberships by reference id %s and member id %s", referenceId, memberId), ex));
                });
    }

    @Override
    public Single<Membership> addOrUpdate(Membership membership, User principal) {
        LOGGER.debug("Add or update membership {}", membership);

        return checkMember(membership)
                .andThen(checkRole(membership.getRole(), membership.getReferenceType()))
                .andThen(membershipRepository.findByReferenceAndMember(membership.getReferenceId(), membership.getMemberId())
                        .map(Optional::of)
                        .defaultIfEmpty(Optional.empty())
                        .flatMapSingle(optMembership -> {
                            if (!optMembership.isPresent()) {
                                // add membership
                                Membership newMembership = new Membership();
                                newMembership.setId(RandomString.generate());
                                newMembership.setDomain(membership.getDomain());
                                newMembership.setMemberId(membership.getMemberId());
                                newMembership.setMemberType(membership.getMemberType());
                                newMembership.setReferenceId(membership.getReferenceId());
                                newMembership.setReferenceType(membership.getReferenceType());
                                newMembership.setRole(membership.getRole());
                                newMembership.setCreatedAt(new Date());
                                newMembership.setUpdatedAt(newMembership.getCreatedAt());
                                return membershipRepository.create(newMembership)
                                        // create event for sync process
                                        .flatMap(membership1 -> {
                                            Event event = new Event(Type.MEMBERSHIP, new Payload(membership1.getId(), membership1.getDomain(), Action.CREATE));
                                            return eventService.create(event).flatMap(__ -> Single.just(membership1));
                                        })
                                        .onErrorResumeNext(ex -> {
                                            if (ex instanceof AbstractManagementException) {
                                                return Single.error(ex);
                                            }
                                            LOGGER.error("An error occurs while trying to create membership {}", membership, ex);
                                            return Single.error(new TechnicalManagementException(String.format("An error occurs while trying to create membership %s", membership), ex));
                                        })
                                        .doOnSuccess(membership1 -> auditService.report(AuditBuilder.builder(MembershipAuditBuilder.class).principal(principal).type(EventType.MEMBERSHIP_CREATED).membership(membership1)))
                                        .doOnError(throwable -> auditService.report(AuditBuilder.builder(DomainAuditBuilder.class).principal(principal).type(EventType.MEMBERSHIP_CREATED).throwable(throwable)));
                            } else {
                                // update membership
                                Membership oldMembership = optMembership.get();
                                Membership updateMembership = new Membership(oldMembership);
                                updateMembership.setRole(membership.getRole());
                                updateMembership.setUpdatedAt(new Date());
                                return membershipRepository.update(updateMembership)
                                        // create event for sync process
                                        .flatMap(membership1 -> {
                                            Event event = new Event(Type.MEMBERSHIP, new Payload(membership1.getId(), membership1.getDomain(), Action.UPDATE));
                                            return eventService.create(event).flatMap(__ -> Single.just(membership1));
                                        })
                                        .onErrorResumeNext(ex -> {
                                            if (ex instanceof AbstractManagementException) {
                                                return Single.error(ex);
                                            }
                                            LOGGER.error("An error occurs while trying to update membership {}", oldMembership, ex);
                                            return Single.error(new TechnicalManagementException(String.format("An error occurs while trying to update membership %s", oldMembership), ex));
                                        })
                                        .doOnSuccess(membership1 -> auditService.report(AuditBuilder.builder(MembershipAuditBuilder.class).principal(principal).type(EventType.MEMBERSHIP_UPDATED).oldValue(oldMembership).membership(membership1)))
                                        .doOnError(throwable -> auditService.report(AuditBuilder.builder(DomainAuditBuilder.class).principal(principal).type(EventType.MEMBERSHIP_UPDATED).throwable(throwable)));
                            }
                        })
                );
    }

    @Override
    public Single<Map<String, Map<String, Object>>> getMetadata(List<Membership> memberships) {
        if (memberships == null || memberships.isEmpty()) {
            return Single.just(Collections.emptyMap());
        }

        List<String> userIds = memberships.stream().filter(membership -> MemberType.USER.equals(membership.getMemberType())).map(Membership::getMemberId).distinct().collect(Collectors.toList());
        List<String> groupIds = memberships.stream().filter(membership -> MemberType.GROUP.equals(membership.getMemberType())).map(Membership::getMemberId).distinct().collect(Collectors.toList());
        List<String> roleIds = memberships.stream().map(Membership::getRole).distinct().collect(Collectors.toList());

        return Single.zip(userService.findByIdIn(userIds), groupService.findByIdIn(groupIds), roleService.findByIdIn(roleIds), (users, groups, roles) -> {
            Map<String, Map<String, Object>> metadata = new HashMap<>();
            metadata.put("users", users.stream().collect(Collectors.toMap(user -> user.getId(), user -> convert(user))));
            metadata.put("groups", groups.stream().collect(Collectors.toMap(group -> group.getId(), group -> convert(group))));
            metadata.put("roles", roles.stream().collect(Collectors.toMap(Role::getId, Function.identity())));
            return metadata;
        });
    }

    @Override
    public Completable delete(String membershipId, User principal) {
        LOGGER.debug("Delete membership {}", membershipId);

        return membershipRepository.findById(membershipId)
                .switchIfEmpty(Maybe.error(new MembershipNotFoundException(membershipId)))
                .flatMapCompletable(membership -> membershipRepository.delete(membershipId)
                        .andThen(Completable.fromSingle(eventService.create(new Event(Type.MEMBERSHIP, new Payload(membership.getId(), membership.getDomain(), Action.DELETE)))))
                        .doOnComplete(() -> auditService.report(AuditBuilder.builder(MembershipAuditBuilder.class).principal(principal).type(EventType.MEMBERSHIP_DELETED).membership(membership)))
                        .doOnError(throwable -> auditService.report(AuditBuilder.builder(MembershipAuditBuilder.class).principal(principal).type(EventType.MEMBERSHIP_DELETED).throwable(throwable)))
                )
                .onErrorResumeNext(ex -> {
                    if (ex instanceof AbstractManagementException) {
                        return Completable.error(ex);
                    }
                    LOGGER.error("An error occurs while trying to delete membership: {}", membershipId, ex);
                    return Completable.error(new TechnicalManagementException(
                            String.format("An error occurs while trying to delete membership: %s", membershipId), ex));
                });
    }

    private Member convert(io.gravitee.am.model.User user) {
        Member member = new Member();
        member.setId(user.getId());
        member.setDisplayName(user.getDisplayName());
        return member;
    }

    private Member convert(Group group) {
        Member member = new Member();
        member.setId(group.getId());
        member.setDisplayName(group.getName());
        return member;
    }

    /**
     * Member must exist and be part of the platform users/groups (i.e master domain)
     * @param membership
     * @return
     */
    private Completable checkMember(Membership membership) {
        if (MemberType.USER.equals(membership.getMemberType())) {
            return userService.findById(membership.getMemberId())
                    .switchIfEmpty(Maybe.error(new UserNotFoundException(membership.getMemberId())))
                    .flatMap(user -> domainService.findById(user.getDomain()))
                    .switchIfEmpty(Maybe.error(new InvalidUserException("Invalid user domain")))
                    .map(domain -> {
                        if (!domain.isMaster()) {
                            throw new UserNotFoundException(membership.getMemberId());
                        }
                        return domain;
                    })
                    .toSingle().toCompletable();
        } else {
            return groupService.findById(membership.getMemberId())
                    .switchIfEmpty(Maybe.error(new GroupNotFoundException(membership.getMemberId())))
                    .flatMap(user -> domainService.findById(user.getDomain()))
                    .switchIfEmpty(Maybe.error(new InvalidGroupException("Invalid group domain")))
                    .map(domain -> {
                        if (!domain.isMaster()) {
                            throw new GroupNotFoundException(membership.getMemberId());
                        }
                        return domain;
                    })
                    .toSingle().toCompletable();
        }
    }

    /**
     * Role must exists and be of the platform roles (i.e master domain)
     * @param role
     * @return
     */
    private Completable checkRole(String role, ReferenceType referenceType) {
        return roleService.findById(role)
                .switchIfEmpty(Maybe.error(new RoleNotFoundException(role)))
                .flatMap(role1 -> domainService.findById(role1.getDomain())
                        .switchIfEmpty(Maybe.error(new InvalidRoleException("Invalid role domain")))
                        .map(domain -> {
                            if (!domain.isMaster()) {
                                throw new RoleNotFoundException(role);
                            }
                            try {
                                if (!RoleScope.valueOf(role1.getScope()).name().equals(referenceType.name())) {
                                    throw new InvalidRoleException("Invalid role scope");
                                }
                            } catch (Exception ex) {
                                throw new InvalidRoleException(ex.getMessage());
                            }
                            return domain;
                        })
                )
                .toSingle().toCompletable();
    }
}