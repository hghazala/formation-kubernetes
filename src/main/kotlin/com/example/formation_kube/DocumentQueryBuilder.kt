package com.stonal.documentstorage.infrastructure.jpa.repositories.document

import com.stonal.documentstorage.domain.common.ClassificationStatus
import com.stonal.documentstorage.domain.common.PageResult
import com.stonal.documentstorage.domain.common.Pageable
import com.stonal.documentstorage.infrastructure.api.document.AuthorizationsSearchFilter
import com.stonal.documentstorage.infrastructure.api.document.DocumentSearchFilter
import com.stonal.documentstorage.infrastructure.api.document.DocumentStatus
import com.stonal.documentstorage.infrastructure.api.document.SearchedDocumentClass
import com.stonal.documentstorage.infrastructure.api.document.SearchedFolder
import com.stonal.documentstorage.infrastructure.jpa.repositories.document.DocumentQueryBuilder.Companion.EMPTY
import com.stonal.documentstorage.infrastructure.jpa.repositories.document.WhereOperator.*
import io.hypersistence.utils.hibernate.type.array.StringArrayType
import jakarta.persistence.EntityManager
import jakarta.persistence.Tuple
import org.hibernate.query.Query

class DocumentQueryBuilder {
    private val select = mutableListOf<String>()

    // Tables is a map of <tableName, tableAlias, usage>, usage = SELECT or JOIN
    private val tables = mutableListOf<Table>()

    // Where is a map of list of WhereClause (all where clause will be with AND). See later for OR
    private val where = mutableListOf<Where>()

    // Joins is a set of TableJoin
    private val joins = mutableSetOf<TableJoin>()
    private var pageable: Pageable? = null

    // BASE QUERY METHODS
    private fun select(vararg fields: String): DocumentQueryBuilder {
        if (fields[0] == "*") {
            select.add("*")
        } else {
            select.addAll(fields)
        }
        return this
    }

    private fun fromTable(
        tableName: String,
        tableAlias: String,
    ): DocumentQueryBuilder {
        tables.add(
            Table(
                tableName = tableName,
                tableAlias = tableAlias,
                usage = "SELECT",
            ),
        )
        return this
    }

    private fun leftJoin(
        targetTableName: String,
        targetTableAlias: String,
        targetTableJoinColumn: String,
        sourceTableAlias: String,
        sourceTableJoinColumn: String,
        shouldApply: () -> Boolean = { true }
    ): DocumentQueryBuilder {
        if (shouldApply()) {
            tables.add(
                Table(
                    tableName = targetTableName,
                    tableAlias = targetTableAlias,
                    usage = "JOIN",
                ),
            )
            joins.add(
                LeftJoin(
                    targetTableName = targetTableName,
                    targetTableAlias = targetTableAlias,
                    targetTableJoinColumn = targetTableJoinColumn,
                    sourceTableAlias = sourceTableAlias,
                    sourceTableJoinColumn = sourceTableJoinColumn,
                ),
            )
        }
        return this
    }

    private fun leftJoinLateral(
        instructions: String,
        targetTableAlias: String,
        doApply: () -> Boolean
    ): DocumentQueryBuilder {
        if (doApply()) {
            joins.add(
                LeftJoinLateral(
                    instructions = instructions,
                    targetTableAlias = targetTableAlias
                )
            )
        }
        return this
    }

    private fun crossJoin(
        instructions: String,
        targetTableAlias: String,
        doApply: () -> Boolean
    ): DocumentQueryBuilder {
        if (doApply()) {
            joins.add(
                CrossJoin(
                    instructions = instructions,
                    targetTableAlias = targetTableAlias
                )
            )
        }
        return this
    }

    private fun withPageable(pageable: Pageable?): DocumentQueryBuilder {
        this.pageable = pageable
        return this
    }

    // WHERE CLAUSES
    private fun withAuthorizations(authorizationsSearchFilter: AuthorizationsSearchFilter?): DocumentQueryBuilder {
        authorizationsSearchFilter
            ?.let { (authorizationGroupIdentifiers, isSuperAdmin, hasDocumentAllAccess, authorityOnAssets) ->
                if (isSuperAdmin || hasDocumentAllAccess) {
                    return this
                }

                where.add(
                    Where(
                        tableAlias = tables.find("document_references")!!.tableAlias,
                        targetField = "referenced_entity_identifier",
                        operator = IN,
                        parameter = "referencedEntityIdentifier",
                        value = authorityOnAssets,
                        ands = listOf(
                            Where(
                                tableAlias = tables.find("document_references")!!.tableAlias,
                                targetField = "documentation",
                                operator = IS_NULL,
                                ors = listOf(
                                    Where(
                                        tableAlias = tables.find("document_references")!!.tableAlias,
                                        targetField = "folder",
                                        operator = IS_NULL,
                                    ),
                                ),
                            ),
                        ),
                        ors = listOf(
                            Where(
                                tableAlias = tables.find("documentations")!!.tableAlias,
                                targetField = "authorizationgroups",
                                operator = ARRAY_IN,
                                parameter = "authorizationgroups",
                                value = authorizationGroupIdentifiers,
                                ands = listOf(
                                    Where(
                                        tableAlias = tables.find("folders", "f3")!!.tableAlias,
                                        targetField = "authorizationgroups",
                                        operator = ARRAY_IN,
                                        parameter = "authorizationgroups",
                                        value = authorizationGroupIdentifiers,
                                    ),
                                ),
                            ),
                        ),
                    ),
                )
            }
        return this
    }

    private fun withOrganization(organizationCode: String?): DocumentQueryBuilder {
        organizationCode
            ?.let { code ->
                tables
                    .find("documents")
                    ?.let { table ->
                        where.add(
                            Where(
                                tableAlias = table.tableAlias,
                                targetField = "organization_code",
                                operator = EQUALS,
                                parameter = "organizationCode",
                                value = code,
                            ),
                        )
                    }
            }
        return this
    }

    private fun withName(name: String?): DocumentQueryBuilder {
        name
            ?.let {
                tables
                    .find("documents")
                    ?.let { table ->
                        where.add(
                            Where(
                                tableAlias = table.tableAlias,
                                targetField = "name",
                                operator = LIKE,
                                parameter = "documentname",
                                value = "%${it.lowercase()}%",
                            ),
                        )
                    }
            }
        return this
    }

    private fun withTags(tags: Set<String>?): DocumentQueryBuilder {
        tags?.let {
            tables
                .find("tags")
                ?.let { table ->
                    where.add(
                        Where(
                            tableAlias = table.tableAlias,
                            targetField = "name",
                            operator = IN,
                            parameter = "tags",
                            value = it,
                        ),
                    )
                }
        }
        return this
    }

    private fun withProperties(properties: Map<String, String?>?): DocumentQueryBuilder {
        properties?.forEach {
            tables
                .find("documents")
                ?.let { table ->
                    where.add(
                        Where(
                            tableAlias = table.tableAlias,
                            targetField = "properties->>'${it.key}'",
                            operator = JSON_EXPLORE_EQUALS_IGNORE_CASE,
                            parameter = "properties",
                            value = it.value?.lowercase(),
                        ),
                    )
                }

        }
        return this
    }

    // ATTENTION : Ce filtre ne va fonctionner que pour la documentReference qui rempli les critères de recherche. Exemple:
    /*
        Un document qui a plusieurs references dont une avec un dossier null et un autre non null.
        Si on recherche pour DocumentStatus.ACTION_REQUIRED, on va trouver un ligne avec dossier null mais pas la 2è projection.
        On va du coup, quand même trouver le document MAIS il ne va y avoir que la documentReference concernée par le filtre.

        Avis de Cyril : à priori c'est quand même ok parce qu'on va chercher en fonction d'un contexte qui est celui de voir si on peut avoir un des status. Donc ok.

        Si c'est pas OK ce filtre, alors il faudra le faire en Java. Malheureusement, on n'est pas avec le mapping Hibernate donc on ne peut pas faire comme avec QueryDSL.
     */
    private fun withStatus(status: String?): DocumentQueryBuilder {
        DocumentStatus
            .from(status)
            ?.let { documentStatus: DocumentStatus ->
                val statusWhere = when (documentStatus) {
                    DocumentStatus.IMPORT,
                    DocumentStatus.CLASSIFICATION,
                    DocumentStatus.METADATA_EXTRACTION,
                    -> Where(
                        tableAlias = tables.find("documents")!!.tableAlias,
                        targetField = "classification_status",
                        operator = EQUALS,
                        parameter = "classificationStatus",
                        value = documentStatus.toClassificationStatus()!!.value,
                    )

                    DocumentStatus.ACTION_REQUIRED -> Where(
                        tableAlias = tables.find("documents")!!.tableAlias,
                        targetField = "classification_status",
                        operator = IN,
                        parameter = "classificationStatus",
                        value = listOf(
                            ClassificationStatus.COMPLETED.value,
                            ClassificationStatus.CANNOT_BE_TREATED.value,
                        ),
                        ands = listOf(
                            Where(
                                tableAlias = tables.find("document_references")!!.tableAlias,
                                targetField = "folder",
                                operator = IS_NULL,
                            ),
                            Where(
                                tableAlias = tables.find("documents")!!.tableAlias,
                                targetField = "manually_validated_folder_id",
                                operator = IS_NULL,
                            ),
                        ),
                    )

                    DocumentStatus.MANUAL -> Where(
                        tableAlias = tables.find("document_references")!!.tableAlias,
                        targetField = "folder",
                        operator = IS_NOT_NULL,
                        ands = listOf(
                            Where(
                                tableAlias = tables.find("documents")!!.tableAlias,
                                targetField = "manually_validated_folder_id",
                                operator = IS_NOT_NULL,
                            ),
                        ),
                    )

                    DocumentStatus.AI -> Where(
                        tableAlias = tables.find("documents")!!.tableAlias,
                        targetField = "classification_status",
                        operator = EQUALS,
                        parameter = "classificationStatus",
                        value = ClassificationStatus.COMPLETED.value,
                        ands = listOf(
                            Where(
                                tableAlias = tables.find("document_references")!!.tableAlias,
                                targetField = "folder",
                                operator = IS_NOT_NULL,
                            ),
                            Where(
                                tableAlias = tables.find("documents")!!.tableAlias,
                                targetField = "predicted_folder_id",
                                operator = IS_NOT_NULL,
                            ),
                            Where(
                                tableAlias = tables.find("documents")!!.tableAlias,
                                targetField = "manually_validated_folder_id",
                                operator = IS_NULL,
                            ),
                        ),
                    )
                }
                where.add(statusWhere)
            }
        return this
    }

    private fun whenDeleted(deleted: Boolean?): DocumentQueryBuilder {
        deleted?.let {
            tables
                .find("documents")
                ?.let { table ->
                    where.add(
                        Where(
                            tableAlias = table.tableAlias,
                            targetField = "deleted",
                            operator = EQUALS,
                            parameter = "deleted",
                            value = it,
                        ),
                    )
                }
        }
        return this
    }

    private fun withAssets(assetIdentifiers: Set<String>?): DocumentQueryBuilder {
        assetIdentifiers?.let {
            tables
                .find("assets")
                ?.let { table ->
                    where.add(
                        Where(
                            tableAlias = table.tableAlias,
                            targetField = "identifier",
                            operator = IN,
                            parameter = "assetidentifiers",
                            value = it,
                        ),
                    )
                }
        }
        return this
    }

    private fun withRentedUnits(rentedUnits: Set<String>?): DocumentQueryBuilder {
        rentedUnits?.let {
            tables
                .find("documents")
                ?.let { table ->
                    where.add(
                        Where(
                            tableAlias = table.tableAlias,
                            targetField = "rented_units",
                            operator = JSON_EXPLORE_KEYS_ANY_EXISTS,
                            parameter = "rentedunits",
                            value = it,
                        ),
                    )
                }
        }
        return this
    }

    private fun withTenants(tenants: Set<String>?): DocumentQueryBuilder {
        tenants?.let {
            tables
                .find("documents")
                ?.let { table ->
                    where.add(
                        Where(
                            tableAlias = table.tableAlias,
                            targetField = "tenants",
                            operator = JSON_EXPLORE_KEYS_ANY_EXISTS,
                            parameter = "tenants",
                            value = it,
                        ),
                    )
                }
        }
        return this
    }

    private fun withDocumentations(documentationIdentifiers: Set<String>?): DocumentQueryBuilder {
        documentationIdentifiers?.let {
            tables
                .find("documentations")
                ?.let { table ->
                    where.add(
                        Where(
                            tableAlias = table.tableAlias,
                            targetField = "identifier",
                            operator = IN,
                            parameter = "documentationidentifiers",
                            value = it,
                        ),
                    )
                }
        }
        return this
    }

    private fun withFolderReferences(folderIdentifiers: Set<String>?): DocumentQueryBuilder {
        folderIdentifiers?.let {
            tables
                .find("folders", "f3")
                ?.let { table ->
                    where.add(
                        Where(
                            tableAlias = table.tableAlias,
                            targetField = "identifier",
                            operator = IN,
                            parameter = "folderreferenceidentifiers",
                            value = it,
                        ),
                    )
                }
        }
        return this
    }

    private fun inFolder(searchedFolder: SearchedFolder?): DocumentQueryBuilder {
        searchedFolder?.let {
            tables
                .find("folders", "f3")
                ?.let { table ->
                    where.add(
                        Where(
                            tableAlias = table.tableAlias,
                            targetField = """f3.names->>'${it.locale}'""",
                            operator = JSON_EXPLORE_LIKE_IGNORE_CASE,
                            parameter = "foldername",
                            value = "%${it.name.lowercase()}%",
                        ),
                    )
                }
        }
        return this
    }

    private fun hasDocumentClassName(searchedDocumentClass: SearchedDocumentClass?): DocumentQueryBuilder {
        searchedDocumentClass?.let {
            tables
                .find("document_classes", "dc")
                ?.let { table ->
                    where.add(
                        Where(
                            tableAlias = table.tableAlias,
                            targetField = """dc.names->>'${it.locale}'""",
                            operator = JSON_EXPLORE_LIKE_IGNORE_CASE,
                            parameter = "documentclassname",
                            value = "%${it.name.lowercase()}%",
                        )
                    )
                }
        }
        return this
    }

    // QUERY BUILDER METHODS
    private fun joinsAsSQL(): String {
        return joins.fold(INIT) { acc, join ->
            when (join) {
                is InnerJoin -> acc + "JOIN ${join.targetTableName} ${join.targetTableAlias} ON ${join.targetTableAlias}.${join.targetTableJoinColumn} = ${join.sourceTableAlias}.${join.sourceTableJoinColumn} "
                is LeftJoin -> acc + "LEFT JOIN ${join.targetTableName} ${join.targetTableAlias} ON ${join.targetTableAlias}.${join.targetTableJoinColumn} = ${join.sourceTableAlias}.${join.sourceTableJoinColumn} "
                is CrossJoin -> acc + "CROSS JOIN ${join.instructions} ${join.targetTableAlias} "
                is LeftJoinLateral -> acc + "LEFT JOIN LATERAL ${join.instructions} ${join.targetTableAlias} ON TRUE "
                else -> acc
            }
        }
    }

    fun whereAsSQL(): String {
        val concatenatedWhere = where
            .joinToString(" AND ") { it.toSQL() }
        return "WHERE $concatenatedWhere"
    }

    private fun paginationAsSQL(): String {
        if (pageable == null) {
            return EMPTY
        }

        val order = if (pageable?.sort?.fieldOrder?.isEmpty() == false) {
            " ORDER BY " + pageable!!
                .sort
                .fieldOrder
                .map { (field, order) ->
                    val fieldName = when (field) {
                        "creationDate" -> "creation_date"
                        else -> field
                    }

                    "${tables.find("documents")?.tableAlias}.$fieldName ${order.name} "
                }
                .joinToString(", ")
        } else {
            EMPTY
        }

        return "$order LIMIT ${pageable!!.pageSize} OFFSET ${pageable!!.pageNumber?.times((pageable!!.pageSize ?: 0))}"
    }

    fun sqlQuery(): String {
        var query = "SELECT ${select.joinToString(", ")} " +
            "FROM ${tables.findSelect()?.tableName} ${tables.findSelect()?.tableAlias} "

        query += joinsAsSQL()
        query += whereAsSQL()
        query += paginationAsSQL()

        return query
    }

    private fun sqlQueryForCount(): String {
        var query = "SELECT COUNT(DISTINCT(${tables.findSelect()?.tableAlias}.id)) " +
            "FROM ${tables.findSelect()?.tableName} ${tables.findSelect()?.tableAlias} "

        query += joinsAsSQL()
        query += whereAsSQL()

        return query
    }

    private fun setNativeQueryParameter(query: Query<*>) {
        where.forEach {
            it.setNativeQueryParameter(query)
        }
    }

    companion object {
        const val EMPTY = ""
        const val INIT = EMPTY

        fun <T> search(
            entityManager: EntityManager,
            documentSearchFilter: DocumentSearchFilter,
            pageable: Pageable,
            mapper: (Tuple) -> T,
        ): PageResult<T> {
            val documentQueryBuilder = applyFilters(documentSearchFilter, pageable)
            val queryResult = query(entityManager, documentQueryBuilder, mapper)
            val count = count(entityManager, documentQueryBuilder)

            return PageResult(
                result = queryResult,
                total = count,
                pageable = pageable,
            )
        }

        fun applyFilters(
            documentSearchFilter: DocumentSearchFilter,
            pageable: Pageable? = null,
        ): DocumentQueryBuilder {
            return baseDocumentsQuery(documentSearchFilter)
                .withPageable(pageable)
                .withAuthorizations(documentSearchFilter.authorizations)
                .withOrganization(documentSearchFilter.organizationCode)
                .withName(documentSearchFilter.name)
                .withTags(documentSearchFilter.tags)
                .withProperties(documentSearchFilter.properties)
                .withStatus(documentSearchFilter.documentStatus)
                .whenDeleted(documentSearchFilter.deleted)
                .withAssets(documentSearchFilter.assetIdentifiers)
                .withRentedUnits(documentSearchFilter.rentedUnitIdentifiers)
                .withTenants(documentSearchFilter.tenantIdentifiers)
                .withDocumentations(documentSearchFilter.documentationIdentifiers)
                .withFolderReferences(documentSearchFilter.folderIdentifiers)
                .inFolder(documentSearchFilter.folder)
                .hasDocumentClassName(documentSearchFilter.documentClass)
        }

        private fun <T> query(
            entityManager: EntityManager,
            documentQueryBuilder: DocumentQueryBuilder,
            mapper: (Tuple) -> T,
        ): List<T> {
            val query = entityManager
                .createNativeQuery(documentQueryBuilder.sqlQuery().trimIndent(), Tuple::class.java)
                .unwrap(Query::class.java)
            documentQueryBuilder.setNativeQueryParameter(query)

            return query
                .resultList
                .map { mapper(it as Tuple) }
        }

        private fun count(
            entityManager: EntityManager,
            documentQueryBuilder: DocumentQueryBuilder,
        ): Long {
            val countQuery = entityManager
                .createNativeQuery(documentQueryBuilder.sqlQueryForCount().trimIndent(), Long::class.java)
                .unwrap(Query::class.java)
            documentQueryBuilder.setNativeQueryParameter(countQuery)

            return countQuery.singleResult as Long
        }

        private fun baseDocumentsQuery(documentSearchFilter: DocumentSearchFilter) = DocumentQueryBuilder()
            .select(
                "d.id as id",
                "d.identifier as identifier",
                "d.organization_code as organizationCode",
                "d.name as name",
                "d.classification_status as classificationStatus",
                "dc.identifier as documentClassIdentifier",
                "dc.code as documentClassCode",
                "dc.description as documentClassDescription",
                "dc.names as documentClassNames",
                "pdc.identifier as predictedDocumentClassIdentifier",
                "pdc.code as predictedDocumentClassCode",
                "pdc.description as predictedDocumentClassDescription",
                "pdc.names as predictedDocumentClassNames",
                "d.confidence_index as confidenceIndex",
                "d.properties as properties",
                "d.rented_units as rentedUnits",
                "d.tenants as tenants",
                "d.tags as tags",
                "d.suggested_metadatas as suggestedMetadata",
                "d.metadata as metadata",
                "d.s3_identifier as s3Identifier",
                "d.content_type as contentType",
                "d.hash_sha256 as hash",
                "d.creation_date as creationDate",
                "d.update_date as updateDate",
                "f.identifier as predictedFolderIdentifier",
                "f.names as predictedFolderNames",
                "f2.identifier as manuallyValidatedFolderIdentifier",
                "f2.names as manuallyValidatedFolderNames",
                "dr.referenced_entity_type as referencedEntityType",
                "dr.referenced_entity_identifier as referencedEntityIdentifier",
                "f3.identifier as referenceFolderIdentifier",
                "f3.names as referenceFolderNames",
                "d2.identifier as referenceDocumentationIdentifier",
                "t.name as tagName",
                "t.color as tagColor",
            )
            .fromTable("documents", "d")
            .leftJoin("document_references", "dr", "document", "d", "id")
            .leftJoin("assets", "a", "identifier", "dr", "referenced_entity_identifier") { documentSearchFilter.assetIdentifiers != null }
            .leftJoin("folders", "f3", "id", "dr", "folder")
            .leftJoin("folders", "f", "id", "d", "predicted_folder_id")
            .leftJoin("folders", "f2", "id", "d", "manually_validated_folder_id")
            .leftJoin("documentations", "d2", "id", "dr", "documentation")
            .leftJoin("document_classes", "dc", "id", "d", "document_class_id")
            .leftJoin("document_classes", "pdc", "id", "d", "predicted_document_class_id")
            .leftJoin("documents_tags", "dt", "document_id", "d", "id")
            .leftJoin("tags", "t", "id", "dt", "tag_id")
    }
}

interface TableJoin

data class InnerJoin(
    val targetTableName: String,
    val targetTableAlias: String,
    val targetTableJoinColumn: String,
    val sourceTableAlias: String,
    val sourceTableJoinColumn: String,
) : TableJoin

data class LeftJoin(
    val targetTableName: String,
    val targetTableAlias: String,
    val targetTableJoinColumn: String,
    val sourceTableAlias: String,
    val sourceTableJoinColumn: String,
) : TableJoin

data class LeftJoinLateral(
    val instructions: String,
    val targetTableAlias: String
) : TableJoin

data class CrossJoin(
    val instructions: String,
    val targetTableAlias: String
) : TableJoin

data class Where(
    val tableAlias: String,
    val targetField: String,
    val operator: WhereOperator,
    val parameter: String? = null,
    val value: Any? = null,
    val ands: List<Where> = emptyList(),
    val ors: List<Where> = emptyList(),
) {
    fun toSQL(): String {
        var clause = "(${clauseToSQL()})"
        val andsAsSql = ands.joinToString(" AND ") { it.toSQL() }
        val orsAsSql = ors.joinToString(" OR ") { it.toSQL() }

        if (ands.isNotEmpty()) {
            clause = "(${clause} AND $andsAsSql)"
        }
        if (ors.isNotEmpty()) {
            clause += " OR $orsAsSql"
        }

        return "($clause)"
    }

    private fun clauseToSQL(): String {
        return when (operator) {
            IS_NULL, IS_NOT_NULL -> "$tableAlias.$targetField ${operator.sqlOperator}"
            JSON_ARRAY_CONTAINS -> "$tableAlias.$targetField ${operator.sqlOperator} $value"
            JSON_ARRAY_FIRST_VALUE_LIKE_IGNORE_CASE -> "lower($tableAlias.${
                targetField.split(
                    '=',
                )[0]
            }[0]${operator.sqlOperator}'${targetField.split('=')[1]}') LIKE :$parameter"
            JSON_EXPLORE_KEYS_ANY_EXISTS -> "$tableAlias.$targetField ${operator.sqlOperator} :$parameter"
            JSON_EXPLORE_LIKE_IGNORE_CASE -> "lower($targetField) ${operator.sqlOperator} :$parameter"
            JSON_EXPLORE_EQUALS_IGNORE_CASE -> "lower($targetField) ${operator.sqlOperator} :$parameter"
            JSON_EXPLORE_ARRAY_CONTAINS -> "$targetField ${operator.sqlOperator} $value"
            LIKE -> "lower($tableAlias.$targetField) ${operator.sqlOperator} :$parameter"
            else -> "$tableAlias.$targetField ${operator.sqlOperator} :$parameter"
        }
    }

    fun setNativeQueryParameter(query: Query<*>) {
        if (parameter != null) {
            when (operator) {
                ARRAY_IN, JSON_EXPLORE_KEYS_ANY_EXISTS -> query.setParameter(
                    parameter,
                    (value as Collection<String>).toTypedArray(),
                    StringArrayType.INSTANCE,
                )

                else -> query.setParameter(parameter, value)
            }
        }
        ands.forEach { it.setNativeQueryParameter(query) }
        ors.forEach { it.setNativeQueryParameter(query) }
    }
}

enum class WhereOperator(val sqlOperator: String) {
    EQUALS("="),
    IN("IN"),
    IS("IS"),
    IS_NOT("IS NOT"),
    LIKE("LIKE"),
    IS_NULL("IS NULL"),
    IS_NOT_NULL("IS NOT NULL"),
    ARRAY_IN("&&"),
    JSON_ARRAY_CONTAINS("@>"),
    JSON_ARRAY_FIRST_VALUE_LIKE_IGNORE_CASE("->>"),
    JSON_EXPLORE_LIKE_IGNORE_CASE("LIKE"),
    JSON_EXPLORE_ARRAY_CONTAINS("@>"),
    JSON_EXPLORE_EQUALS_IGNORE_CASE("="),
    JSON_EXPLORE_KEYS_ANY_EXISTS("\\?\\?\\|"),
}

data class Table(
    val tableName: String,
    val tableAlias: String,
    val usage: String, // Usage = "SELECT", "JOIN"
)

fun Collection<Table>.find(tableName: String): Table? {
    return this.find { it.tableName == tableName }
}

fun Collection<Table>.find(
    tableName: String,
    tableAlias: String,
): Table? {
    return this.find { it.tableName == tableName && it.tableAlias == tableAlias }
}

fun Collection<Table>.findSelect(): Table? {
    return this.find { it.usage == "SELECT" }
}

fun String.escape() =
    this
        .replace(";", EMPTY)
        .replace("\'", EMPTY)
        .replace("'", EMPTY)
        .replace(",", EMPTY)
        .replace("=", EMPTY)
