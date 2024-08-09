
import regex


def normalize_text(text):
    only_unicode_text = regex.sub(r'\W+', ' ', text, flags=regex.UNICODE)
    return regex.sub(' +', ' ', only_unicode_text).strip()


def generate_rss(title, description, link, url_rss, last_build_date,
                 language, update_frequency, update_period, items):
    output = '<rss xmlns:sy="http://purl.org/rss/1.0/modules/syndication/" xmlns:wfw="http://wellformedweb.org/CommentAPI/" xmlns:slash="http://purl.org/rss/1.0/modules/slash/" xmlns:atom="http://www.w3.org/2005/Atom" xmlns:content="http://purl.org/rss/1.0/modules/content/" xmlns:dc="http://purl.org/dc/elements/1.1/" version="2.0">'
    output += f'''
    <channel>
        <title>{title}</title>
        <atom:link href="{url_rss}" rel="self" type="application/rss+xml"/>
        <description>{description}</description>
        <link>{link}</link>
        <lastBuildDate>{last_build_date}</lastBuildDate>
        <language>{language}</language>
        <sy:updatePeriod>{update_period}</sy:updatePeriod>
        <sy:updateFrequency>{update_frequency}</sy:updateFrequency>'''

    for item in items:
        # print(item['title'])
        title = normalize_text(item['title'])
        description = normalize_text(item['description'])
        # unicodedata.normalize('NFKC', item['description'])
        output += f'''
        <item>
            <title><![CDATA[ {title} ]]></title>
            <link>{item['link']}</link>
            <pubDate>{item['pubDate']}</pubDate>
            <guid isPermaLink="false">{item['link']}</guid>
            <description>
                <![CDATA[ {description} ]]>
            </description>
        </item>'''
        # output += f'''
        #         <item>
        #             <title>{item['title']}</title>
        #             <link>{item['link']}</link>
        #             <pubDate>{item['pubDate']}</pubDate>
        #             <guid isPermaLink="false">{item['link']}</guid>
        #             <description>
        #                 <![CDATA[ {item['description']} ]]>
        #             </description>
        #         </item>'''

    output += f'''
    </channel>
</rss>'''
    return output
