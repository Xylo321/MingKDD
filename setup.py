from setuptools import find_packages, setup

setup(
    name='MingKDD',
    version='1.0.1',
    url='',
    license='',
    maintainer='kael',
    maintainer_email='congshi.hello@gmail.com',
    description='',
    long_description='',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        "selenium",
        "requests",
        "lxml",
        "cssselect",
        "mingmq",
        "PyMySQL"
    ],
    entry_points="""
    [console_scripts]
    blog_get_article_category_consumer = ming_kdd.mmq.client.blog:main
    blog_get_photo_category_consumer = ming_kdd.mmq.client.image:main
    blog_get_video_category_consumer = ming_kdd.mmq.client.video:main
    blog_add_article_consumer = ming_kdd.mmq.server.blog:main
    blog_add_photo_category_consumer = ming_kdd.mmq.server.image:main
    blog_add_video_category_consumer = ming_kdd.mmq.server.video:main
    """
)
