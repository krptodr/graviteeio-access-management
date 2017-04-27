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
package io.gravitee.am.repository.mongodb.management;

import io.gravitee.am.model.Client;
import io.gravitee.am.repository.exceptions.TechnicalException;
import io.gravitee.am.repository.management.api.ClientRepository;
import io.gravitee.am.repository.mongodb.management.internal.model.ClientMongo;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
@Component
public class MongoClientRepository extends AbstractManagementMongoRepository implements ClientRepository {

    private static final String FIELD_DOMAIN = "domain";
    private static final String FIELD_CLIENT_ID = "clientId";
    private static final String FIELD_IDENTITIES = "identities";

    @Override
    public Set<Client> findByDomain(String domain) throws TechnicalException {
        Query query = new Query();
        query.addCriteria(Criteria.where(FIELD_DOMAIN).is(domain));

        return mongoOperations
                .find(query, ClientMongo.class)
                .stream()
                .map(this::convert)
                .collect(Collectors.toSet());
    }

    @Override
    public Optional<Client> findByClientIdAndDomain(String clientId, String domain) throws TechnicalException {
        Query query = new Query();
        query.addCriteria(
                Criteria.where(FIELD_DOMAIN).is(domain)
                        .andOperator(Criteria.where(FIELD_CLIENT_ID).is(clientId)));

        ClientMongo client = mongoOperations.findOne(query, ClientMongo.class);
        return Optional.ofNullable(convert(client));
    }

    @Override
    public Set<Client> findByIdentityProvider(String identityProvider) {
        Query query = new Query();
        query.addCriteria(Criteria.where(FIELD_IDENTITIES).is(identityProvider));

        return mongoOperations
                .find(query, ClientMongo.class)
                .stream()
                .map(this::convert)
                .collect(Collectors.toSet());
    }

    @Override
    public Optional<Client> findById(String client) throws TechnicalException {
        return Optional.ofNullable(convert(mongoOperations.findById(client, ClientMongo.class)));
    }

    @Override
    public Client create(Client item) throws TechnicalException {
        ClientMongo domain = convert(item);
        mongoOperations.save(domain);
        return convert(domain);
    }

    @Override
    public Client update(Client item) throws TechnicalException {
        ClientMongo domain = convert(item);
        mongoOperations.save(domain);
        return convert(domain);
    }

    @Override
    public void delete(String id) throws TechnicalException {
        ClientMongo client = mongoOperations.findById(id, ClientMongo.class);
        mongoOperations.remove(client);
    }

    private Client convert(ClientMongo clientMongo) {
        if (clientMongo == null) {
            return null;
        }

        Client client = new Client();
        client.setId(clientMongo.getId());
        client.setClientId(clientMongo.getClientId());
        client.setClientSecret(clientMongo.getClientSecret());
        client.setAccessTokenValiditySeconds(clientMongo.getAccessTokenValiditySeconds());
        client.setRefreshTokenValiditySeconds(clientMongo.getRefreshTokenValiditySeconds());
        client.setRedirectUris(clientMongo.getRedirectUris());
        client.setScopes(clientMongo.getScopes());
        client.setEnabled(clientMongo.isEnabled());
        client.setIdentities(clientMongo.getIdentities());
        client.setDomain(clientMongo.getDomain());
        client.setAuthorizedGrantTypes(clientMongo.getAuthorizedGrantTypes());
        client.setCreatedAt(clientMongo.getCreatedAt());
        client.setUpdatedAt(clientMongo.getUpdatedAt());
        return client;
    }

    private ClientMongo convert(Client client) {
        if (client == null) {
            return null;
        }

        ClientMongo clientMongo = new ClientMongo();
        clientMongo.setId(client.getId());
        clientMongo.setClientId(client.getClientId());
        clientMongo.setClientSecret(client.getClientSecret());
        clientMongo.setAccessTokenValiditySeconds(client.getAccessTokenValiditySeconds());
        clientMongo.setRefreshTokenValiditySeconds(client.getRefreshTokenValiditySeconds());
        clientMongo.setRedirectUris(client.getRedirectUris());
        clientMongo.setAuthorizedGrantTypes(client.getAuthorizedGrantTypes());
        clientMongo.setScopes(client.getScopes());
        clientMongo.setEnabled(client.isEnabled());
        clientMongo.setIdentities(client.getIdentities());
        clientMongo.setDomain(client.getDomain());
        clientMongo.setCreatedAt(client.getCreatedAt());
        clientMongo.setUpdatedAt(client.getUpdatedAt());
        return clientMongo;
    }
}