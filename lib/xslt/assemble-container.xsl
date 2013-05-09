<?xml version="1.0" encoding="ISO-8859-1"?>
<xsl:transform xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
	version="2.0" xmlns:redirect="http://xml.apache.org/xalan/redirect"
	extension-element-prefixes="redirect str xalan com dyn" xmlns:str="http://exslt.org/strings"
	xmlns:com="http://exslt.org/common" xmlns:xalan="http://xml.apache.org/xalan"
	xmlns:dyn="http://exslt.org/dynamic">
	
	<xsl:param name="enablername-param"/>
	<xsl:param name="container.tmp-param"/>
	<xsl:param name="rcv.tmp-param"/>
	
	<xsl:output method="xml" version="1.0" omit-xml-declaration="yes"
		encoding="ISO-8859-1" indent="yes" />
	<xsl:variable name='newline'><xsl:text>&#xa;</xsl:text></xsl:variable>
	<xsl:variable name='carriageReturn'><xsl:text>&#13;</xsl:text></xsl:variable>
	<xsl:variable name='tab'><xsl:text>&#9;</xsl:text></xsl:variable>
	<xsl:variable name='space'><xsl:text>&#32;</xsl:text></xsl:variable>
	<xsl:variable name="apos">'</xsl:variable>
	<xsl:variable name="quot">"</xsl:variable>
    <!-- <xsl:variable name="rcv.tmpfile" select="/temp/rcv.tmp"/> 
    <xsl:variable name="rcv.tmpfile">tmp/rcv.tmp</xsl:variable>
    -->
    <xsl:variable name="stackBuilds.tmpfile"     select="/temp/generatedStackBuilds.tmp"/>
	<!-- ******************************************** Initialize ************************************************* -->

	<xsl:template match="/">
		<xsl:message select="Intializing" />		
		<xsl:output method="xml" version="1.0" encoding="ISO-8859-1" />
		<redirect:write file="{$container.tmp-param}">
<container class="com.datasynapse.fabric.container.ExecContainer">
	<runtimeContextTemplate class="com.datasynapse.fabric.common.DefaultRuntimeContext">
		@runtimeContextTemplate-here@
	</runtimeContextTemplate>
	<xsl:apply-templates />
</container>
		</redirect:write>
	</xsl:template>

	<xsl:template match="container">
		<xsl:apply-templates />
	</xsl:template>

	<xsl:template match="include">
		<xsl:variable name="include.file"><xsl:value-of select="./@file"/></xsl:variable>
		<xsl:variable name="include.doc" select="document($include.file,.)" />
		<xsl:apply-templates select="$include.doc/*" />
	</xsl:template>

	<xsl:template match="runtimeContextTemplate">
		<redirect:write file="{$rcv.tmp-param}" append="true">
		 	<xsl:apply-templates />
		</redirect:write>
	</xsl:template>

	<xsl:template match="*">
		<xsl:copy>
			<xsl:apply-templates select="@*|node()"/>
		</xsl:copy>
	</xsl:template>
	
	<xsl:template match="@*|text()|comment()|processing-instruction()">
		<xsl:copy-of select="."/>
	</xsl:template>

</xsl:transform>
